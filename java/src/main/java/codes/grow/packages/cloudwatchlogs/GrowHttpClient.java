package codes.grow.packages.cloudwatchlogs;

import codes.grow.packages.cloudwatchlogs.core.CloudWatchLogsException;
import codes.grow.packages.cloudwatchlogs.core.ConfigException;
import codes.grow.packages.cloudwatchlogs.core.ErrorCode;
import codes.grow.packages.cloudwatchlogs.core.Hooks;
import codes.grow.packages.cloudwatchlogs.core.HttpTransport;
import codes.grow.packages.cloudwatchlogs.core.Logger;
import codes.grow.packages.cloudwatchlogs.core.Redaction;
import codes.grow.packages.cloudwatchlogs.core.TransportConfig;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.Authenticator;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.reactivestreams.FlowAdapters;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpResponseHandler;
import software.amazon.awssdk.http.async.SdkHttpContentPublisher;

/**
 * The HTTP client the AWS SDK uses (SPEC section 2.3 "Cloud SDK services"), built on {@code
 * java.net.http}. The SDK signs, serialises, retries and parses (including event streams); this
 * client sends one attempt with the Grow TLS (CA bundle, client certificate, insecureSkipVerify),
 * proxy and timeout settings, fires the SPEC section 2.5 hooks and logs each attempt at debug
 * level. Live-tail event streams are passed on as they arrive; the per-attempt timeout covers only
 * the response headers of a streaming call.
 */
final class GrowHttpClient implements SdkAsyncHttpClient, SdkHttpClient {
  /** Headers {@code java.net.http} sets itself and rejects. */
  private static final Set<String> RESTRICTED =
      Set.of("host", "content-length", "connection", "expect", "upgrade", "transfer-encoding");

  private static final Pattern ATTEMPT = Pattern.compile("attempt=(\\d+)");
  private static final Pattern MAX = Pattern.compile("max=(\\d+)");
  private static final Pattern CREATE_TIME_NUMBER =
      Pattern.compile("(\"createTime\"\\s*:\\s*)(-?\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?)");

  private final TransportConfig config;
  private final HttpClient client;
  private final AtomicBoolean closed = new AtomicBoolean();
  private final Map<String, Runnable> aborts = new java.util.concurrent.ConcurrentHashMap<>();

  /** Request header that links a live-tail session to its HTTP exchange (see {@link #abort}). */
  static final String SESSION_HEADER = "x-grow-sdk-session";

  GrowHttpClient(TransportConfig config) {
    this.config = config;
    HttpClient.Builder b =
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(config.connectTimeout())
            .followRedirects(HttpClient.Redirect.NEVER);
    if (config.tls().isCustom()) {
      b.sslContext(config.tls().sslContext());
      if (config.tls().insecureSkipVerify()) {
        log(
            "warn",
            "TLS verification is disabled (insecureSkipVerify); never use this in production",
            Map.of());
      }
    }
    Optional<String> proxy = config.proxy();
    if (proxy.isPresent()) {
      URI p = parseProxy(proxy.get());
      int port = p.getPort() > 0 ? p.getPort() : ("https".equals(p.getScheme()) ? 443 : 80);
      b.proxy(ProxySelector.of(InetSocketAddress.createUnresolved(p.getHost(), port)));
      String userInfo = p.getRawUserInfo();
      if (userInfo != null) {
        int colon = userInfo.indexOf(':');
        String user =
            URLDecoder.decode(
                colon < 0 ? userInfo : userInfo.substring(0, colon), StandardCharsets.UTF_8);
        String pass =
            colon < 0
                ? ""
                : URLDecoder.decode(userInfo.substring(colon + 1), StandardCharsets.UTF_8);
        b.authenticator(new ProxyAuthenticator(user, pass.toCharArray()));
      }
    } else {
      b.proxy(HttpClient.Builder.NO_PROXY);
    }
    this.client = b.build();
  }

  private static URI parseProxy(String proxy) {
    try {
      URI p = new URI(proxy);
      if (p.getHost() == null || p.getScheme() == null) {
        throw new ConfigException("config: proxy must be an absolute URL such as http://host:3128");
      }
      return p;
    } catch (URISyntaxException e) {
      throw new ConfigException("config: proxy is not a valid URL", e);
    }
  }

  @Override
  public String clientName() {
    return "GrowJavaHttp";
  }

  /** Parsed {@code amz-sdk-request: attempt=2; max=4}. */
  record Attempt(int attempt, int max) {}

  static Attempt parseAttempt(String header) {
    String v = header == null ? "" : header;
    Matcher a = ATTEMPT.matcher(v);
    Matcher m = MAX.matcher(v);
    return new Attempt(
        a.find() ? Integer.parseInt(a.group(1)) : 1, m.find() ? Integer.parseInt(m.group(1)) : 1);
  }

  private static String operationOf(SdkHttpRequest r) {
    String target = r.firstMatchingHeader("X-Amz-Target").orElse("");
    int dot = target.lastIndexOf('.');
    return dot >= 0 ? target.substring(dot + 1) : target;
  }

  private HttpRequest.Builder build(SdkHttpRequest r, HttpRequest.BodyPublisher body) {
    HttpRequest.Builder b = HttpRequest.newBuilder(r.getUri());
    for (Map.Entry<String, List<String>> h : r.headers().entrySet()) {
      if (RESTRICTED.contains(h.getKey().toLowerCase(Locale.ROOT))) {
        continue;
      }
      for (String v : h.getValue()) {
        b.header(h.getKey(), v);
      }
    }
    b.method(r.method().name(), body);
    // The JDK timeout covers the response headers only, so live-tail sessions (up to 3 h) are not
    // cut while their event stream is open.
    b.timeout(config.requestTimeout());
    return b;
  }

  private static SdkHttpResponse toSdk(HttpResponse<?> resp) {
    SdkHttpResponse.Builder b = SdkHttpResponse.builder().statusCode(resp.statusCode());
    for (Map.Entry<String, List<String>> h : resp.headers().map().entrySet()) {
      if (!h.getKey().startsWith(":")) {
        b.putHeader(h.getKey(), h.getValue());
      }
    }
    return b.build();
  }

  private final class Attempted {
    final SdkHttpRequest request;
    final Attempt attempt;
    final String url;
    final long start = System.nanoTime();

    Attempted(SdkHttpRequest request) {
      this.request = request;
      this.attempt = parseAttempt(request.firstMatchingHeader("amz-sdk-request").orElse(null));
      this.url = Redaction.redactUrl(request.getUri().toString());
      Map<String, List<String>> headers = new LinkedHashMap<>();
      request.forEachHeader((k, v) -> headers.put(k, List.copyOf(v)));
      safe(
          () ->
              config
                  .hooks()
                  .onRequest(
                      new Hooks.RequestInfo(
                          request.method().name(),
                          url,
                          Redaction.redactHeaderLists(headers),
                          attempt.attempt())));
    }

    void response(HttpResponse<?> resp) {
      long ms = (System.nanoTime() - start) / 1_000_000;
      Map<String, List<String>> headers = resp.headers().map();
      Optional<String> rid = requestId(headers);
      safe(
          () ->
              config
                  .hooks()
                  .onResponse(
                      new Hooks.ResponseInfo(
                          request.method().name(),
                          url,
                          resp.statusCode(),
                          ms,
                          attempt.attempt(),
                          rid)));
      Map<String, Object> fields = new LinkedHashMap<>();
      fields.put("operation", operationOf(request));
      fields.put("status", resp.statusCode());
      fields.put("attempt", attempt.attempt());
      fields.put("durationMs", ms);
      rid.ifPresent(v -> fields.put("requestId", v));
      log("debug", "response", fields);
    }

    void failure(Throwable error) {
      CloudWatchLogsException mapped = attemptError(operationOf(request), error);
      boolean willRetry = attempt.attempt() < attempt.max() && mapped.retryable();
      safe(
          () ->
              config
                  .hooks()
                  .onError(
                      new Hooks.ErrorInfo(
                          request.method().name(), url, mapped, attempt.attempt(), willRetry)));
      log(
          "debug",
          "request failed",
          Map.of(
              "operation",
              operationOf(request),
              "attempt",
              attempt.attempt(),
              "error",
              mapped.code().name()));
    }
  }

  static CloudWatchLogsException attemptError(String operation, Throwable error) {
    Throwable c = error;
    while (c != null && c.getCause() != null && !(c instanceof IOException)) {
      c = c.getCause();
    }
    if (c instanceof HttpTimeoutException || c instanceof InterruptedIOException) {
      return CloudWatchLogsException.of(
          ErrorCode.TIMEOUT, operation + ": attempt timed out", null, null, null, null);
    }
    return CloudWatchLogsException.of(
        ErrorCode.UNAVAILABLE,
        operation
            + ": request failed ("
            + (c == null ? "error" : c.getClass().getSimpleName())
            + ")",
        null,
        null,
        null,
        null);
  }

  static Optional<String> requestId(Map<String, List<String>> headers) {
    for (String name : HttpTransport.REQUEST_ID_HEADERS) {
      for (Map.Entry<String, List<String>> h : headers.entrySet()) {
        if (h.getKey().equalsIgnoreCase(name) && !h.getValue().isEmpty()) {
          return Optional.of(h.getValue().get(0));
        }
      }
    }
    return Optional.empty();
  }

  private static void safe(Runnable r) {
    try {
      r.run();
    } catch (RuntimeException ignored) {
      // hooks and loggers must never break a call
    }
  }

  private void log(String level, String message, Map<String, Object> fields) {
    Logger l = config.logger();
    safe(
        () -> {
          switch (level) {
            case "warn":
              l.warn(message, fields);
              break;
            default:
              l.debug(message, fields);
          }
        });
  }

  // ------------------------------------------------------------------ async

  @Override
  public CompletableFuture<Void> execute(AsyncExecuteRequest request) {
    CompletableFuture<Void> result = new CompletableFuture<>();
    SdkAsyncHttpResponseHandler handler = request.responseHandler();
    if (closed.get()) {
      IOException e = new IOException("HTTP client is closed");
      handler.onError(e);
      result.completeExceptionally(e);
      return result;
    }
    SdkHttpRequest r = request.request();
    SdkHttpContentPublisher content = request.requestContentPublisher();
    long length = content == null ? 0 : content.contentLength().orElse(-1L);
    HttpRequest.BodyPublisher body;
    if (content == null || length == 0) {
      body = HttpRequest.BodyPublishers.noBody();
    } else if (length > 0) {
      body =
          HttpRequest.BodyPublishers.fromPublisher(FlowAdapters.toFlowPublisher(content), length);
    } else {
      body = HttpRequest.BodyPublishers.fromPublisher(FlowAdapters.toFlowPublisher(content));
    }
    Attempted att = new Attempted(r);
    AtomicReference<Flow.Subscription> bodySub = new AtomicReference<>();
    HttpResponse.BodyHandler<Flow.Publisher<List<ByteBuffer>>> bodyHandler =
        "DescribeQueries".equals(operationOf(r))
            ? info ->
                HttpResponse.BodySubscribers.mapping(
                    HttpResponse.BodySubscribers.ofByteArray(),
                    bytes -> single(integerCreateTimes(bytes)))
            : HttpResponse.BodyHandlers.ofPublisher();
    CompletableFuture<HttpResponse<Flow.Publisher<List<ByteBuffer>>>> inflight =
        client.sendAsync(build(r, body).build(), bodyHandler);
    Runnable abort =
        () -> {
          inflight.cancel(true);
          Flow.Subscription s = bodySub.get();
          if (s != null) {
            s.cancel();
          }
        };
    Optional<String> session = r.firstMatchingHeader(SESSION_HEADER);
    session.ifPresent(id -> aborts.put(id, () -> result.cancel(true)));
    result.whenComplete(
        (v, t) -> {
          session.ifPresent(aborts::remove);
          if (result.isCancelled() || t != null) {
            abort.run();
          }
        });
    inflight.whenComplete(
        (resp, err) -> {
          if (err != null) {
            att.failure(err);
            Throwable cause = err.getCause() != null ? err.getCause() : err;
            safe(() -> handler.onError(cause));
            result.completeExceptionally(cause);
            return;
          }
          att.response(resp);
          handler.onHeaders(toSdk(resp));
          handler.onStream(new BodyPublisher(resp.body(), bodySub, result));
        });
    return result;
  }

  /**
   * Moto sends {@code DescribeQueries.createTime} as a float ({@code 1795033644934.42}) where the
   * AWS model has a long; the Java SDK parser rejects it. AWS sends integers, so this only drops a
   * fraction the emulator added.
   */
  static byte[] integerCreateTimes(byte[] body) {
    String text = new String(body, StandardCharsets.UTF_8);
    Matcher m = CREATE_TIME_NUMBER.matcher(text);
    StringBuilder out = new StringBuilder();
    boolean changed = false;
    while (m.find()) {
      String number = m.group(2);
      String whole = number;
      if (number.indexOf('.') >= 0 || number.indexOf('e') >= 0 || number.indexOf('E') >= 0) {
        whole = new java.math.BigDecimal(number).toBigInteger().toString();
        changed = true;
      }
      m.appendReplacement(out, Matcher.quoteReplacement(m.group(1) + whole));
    }
    m.appendTail(out);
    return changed ? out.toString().getBytes(StandardCharsets.UTF_8) : body;
  }

  /** A publisher of one chunk list. */
  static Flow.Publisher<List<ByteBuffer>> single(byte[] bytes) {
    return subscriber ->
        subscriber.onSubscribe(
            new Flow.Subscription() {
              private final AtomicBoolean sent = new AtomicBoolean();

              @Override
              public void request(long n) {
                if (n > 0 && sent.compareAndSet(false, true)) {
                  subscriber.onNext(List.of(ByteBuffer.wrap(bytes)));
                  subscriber.onComplete();
                }
              }

              @Override
              public void cancel() {
                sent.set(true);
              }
            });
  }

  /**
   * Aborts the HTTP exchange of a live-tail session (the SDK does not cancel an open event stream
   * when its future or subscription is cancelled).
   *
   * @param session value of {@link #SESSION_HEADER}
   */
  void abort(String session) {
    Runnable r = aborts.remove(session);
    if (r != null) {
      r.run();
    }
  }

  /** Adapts the JDK body ({@code List<ByteBuffer>} chunks) to the SDK's byte-buffer publisher. */
  static final class BodyPublisher implements Publisher<ByteBuffer> {
    private final Flow.Publisher<List<ByteBuffer>> source;
    private final AtomicReference<Flow.Subscription> holder;
    private final CompletableFuture<Void> done;

    BodyPublisher(
        Flow.Publisher<List<ByteBuffer>> source,
        AtomicReference<Flow.Subscription> holder,
        CompletableFuture<Void> done) {
      this.source = source;
      this.holder = holder;
      this.done = done;
    }

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> s) {
      source.subscribe(new Flattener(s, holder, done));
    }
  }

  /**
   * Flattens chunk lists into single buffers with demand accounting (each requested item is one
   * buffer; extra buffers of a chunk wait for demand).
   */
  static final class Flattener implements Flow.Subscriber<List<ByteBuffer>> {
    private final Subscriber<? super ByteBuffer> downstream;
    private final AtomicReference<Flow.Subscription> holder;
    private final CompletableFuture<Void> done;
    private final java.util.ArrayDeque<ByteBuffer> pending = new java.util.ArrayDeque<>();
    private long demand;
    private boolean upstreamRequested;
    private boolean completed;
    private Throwable failure;
    private boolean terminated;
    private Flow.Subscription upstream;

    Flattener(
        Subscriber<? super ByteBuffer> downstream,
        AtomicReference<Flow.Subscription> holder,
        CompletableFuture<Void> done) {
      this.downstream = downstream;
      this.holder = holder;
      this.done = done;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      upstream = subscription;
      holder.set(subscription);
      downstream.onSubscribe(
          new Subscription() {
            @Override
            public void request(long n) {
              synchronized (Flattener.this) {
                demand = demand + n < 0 ? Long.MAX_VALUE : demand + n;
              }
              drain();
            }

            @Override
            public void cancel() {
              synchronized (Flattener.this) {
                terminated = true;
                pending.clear();
              }
              subscription.cancel();
              done.complete(null);
            }
          });
    }

    @Override
    public void onNext(List<ByteBuffer> items) {
      synchronized (this) {
        upstreamRequested = false;
        for (ByteBuffer b : items) {
          if (b.hasRemaining()) {
            pending.add(b);
          }
        }
      }
      drain();
    }

    @Override
    public void onError(Throwable throwable) {
      synchronized (this) {
        failure = throwable;
        completed = true;
      }
      drain();
    }

    @Override
    public void onComplete() {
      synchronized (this) {
        completed = true;
      }
      drain();
    }

    private void drain() {
      while (true) {
        ByteBuffer next = null;
        boolean finish = false;
        boolean requestMore = false;
        Throwable err = null;
        synchronized (this) {
          if (terminated) {
            return;
          }
          if (demand > 0 && !pending.isEmpty()) {
            next = pending.poll();
            demand--;
          } else if (pending.isEmpty() && completed) {
            terminated = true;
            finish = true;
            err = failure;
          } else if (demand > 0 && pending.isEmpty() && !upstreamRequested) {
            upstreamRequested = true;
            requestMore = true;
          }
        }
        if (next != null) {
          downstream.onNext(next);
          continue;
        }
        if (finish) {
          if (err != null) {
            downstream.onError(err);
            done.completeExceptionally(err);
          } else {
            downstream.onComplete();
            done.complete(null);
          }
          return;
        }
        if (requestMore) {
          upstream.request(1);
        }
        return;
      }
    }
  }

  // ------------------------------------------------------------------ sync

  @Override
  public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
    return new ExecutableHttpRequest() {
      private volatile CompletableFuture<HttpResponse<InputStream>> inflight;

      @Override
      public HttpExecuteResponse call() throws IOException {
        if (closed.get()) {
          throw new IOException("HTTP client is closed");
        }
        SdkHttpRequest r = request.httpRequest();
        HttpRequest.BodyPublisher body =
            request
                .contentStreamProvider()
                .map(p -> HttpRequest.BodyPublishers.ofInputStream(p::newStream))
                .orElse(HttpRequest.BodyPublishers.noBody());
        Attempted att = new Attempted(r);
        try {
          inflight =
              client.sendAsync(build(r, body).build(), HttpResponse.BodyHandlers.ofInputStream());
          HttpResponse<InputStream> resp = inflight.get();
          att.response(resp);
          return HttpExecuteResponse.builder()
              .response(toSdk(resp))
              .responseBody(AbortableInputStream.create(resp.body()))
              .build();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          att.failure(e);
          throw new InterruptedIOException("request interrupted");
        } catch (java.util.concurrent.ExecutionException e) {
          Throwable c = e.getCause() != null ? e.getCause() : e;
          att.failure(c);
          if (c instanceof IOException) {
            throw (IOException) c;
          }
          throw new IOException(c);
        }
      }

      @Override
      public void abort() {
        CompletableFuture<?> f = inflight;
        if (f != null) {
          f.cancel(true);
        }
      }
    };
  }

  @Override
  public void close() {
    closed.set(true);
  }

  /** Supplies proxy credentials only for proxy challenges. */
  private static final class ProxyAuthenticator extends Authenticator {
    private final String user;
    private final char[] password;

    ProxyAuthenticator(String user, char[] password) {
      this.user = user;
      this.password = password;
    }

    @Override
    protected PasswordAuthentication getPasswordAuthentication() {
      if (getRequestorType() != RequestorType.PROXY) {
        return null;
      }
      return new PasswordAuthentication(user, password.clone());
    }
  }
}
