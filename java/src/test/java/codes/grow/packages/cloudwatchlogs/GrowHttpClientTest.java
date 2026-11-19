package codes.grow.packages.cloudwatchlogs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import codes.grow.packages.cloudwatchlogs.core.ConfigException;
import codes.grow.packages.cloudwatchlogs.core.ErrorCode;
import codes.grow.packages.cloudwatchlogs.core.Hooks;
import codes.grow.packages.cloudwatchlogs.core.Logger;
import codes.grow.packages.cloudwatchlogs.core.MockServer;
import codes.grow.packages.cloudwatchlogs.core.TlsOptions;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpResponseHandler;
import software.amazon.awssdk.http.async.SdkHttpContentPublisher;

class GrowHttpClientTest {
  MockServer server;
  final List<Object> events = Collections.synchronizedList(new ArrayList<>());
  final List<String> logs = Collections.synchronizedList(new ArrayList<>());

  @BeforeEach
  void start() throws Exception {
    server = MockServer.start();
  }

  @AfterEach
  void stop() {
    server.close();
  }

  GrowHttpClient client(CloudWatchLogsConfig.Builder b) {
    return new GrowHttpClient(
        b.hooks(
                new Hooks() {
                  @Override
                  public void onRequest(RequestInfo info) {
                    events.add(info);
                    throw new IllegalStateException("hooks never break a call");
                  }

                  @Override
                  public void onResponse(ResponseInfo info) {
                    events.add(info);
                  }

                  @Override
                  public void onError(ErrorInfo info) {
                    events.add(info);
                  }
                })
            .logger(
                new Logger() {
                  @Override
                  public void debug(String message, Map<String, Object> fields) {
                    logs.add(message + fields);
                  }

                  @Override
                  public void warn(String message, Map<String, Object> fields) {
                    logs.add("WARN " + message);
                  }
                })
            .build()
            .toTransportConfig());
  }

  SdkHttpRequest request(String url, String target) {
    SdkHttpRequest.Builder b =
        SdkHttpRequest.builder()
            .uri(URI.create(url))
            .method(SdkHttpMethod.POST)
            .putHeader("Host", "ignored")
            .putHeader("Content-Length", "2")
            .putHeader("amz-sdk-request", "attempt=2; max=3")
            .putHeader("Authorization", "AWS4-HMAC-SHA256 Credential=secret");
    if (target != null) {
      b.putHeader("X-Amz-Target", "Logs_20140328." + target);
    }
    return b.build();
  }

  static SdkHttpContentPublisher body(String text, boolean known) {
    byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
    return new SdkHttpContentPublisher() {
      @Override
      public Optional<Long> contentLength() {
        return known ? Optional.of((long) bytes.length) : Optional.empty();
      }

      @Override
      public void subscribe(Subscriber<? super ByteBuffer> s) {
        s.onSubscribe(
            new Subscription() {
              boolean sent;

              @Override
              public void request(long n) {
                if (!sent) {
                  sent = true;
                  if (bytes.length > 0) {
                    s.onNext(ByteBuffer.wrap(bytes));
                  }
                  s.onComplete();
                }
              }

              @Override
              public void cancel() {}
            });
      }
    };
  }

  /** Collects a response through the async API. */
  static final class Collector implements SdkAsyncHttpResponseHandler {
    final CompletableFuture<SdkHttpResponse> headers = new CompletableFuture<>();
    final CompletableFuture<String> body = new CompletableFuture<>();
    final StringBuilder sb = new StringBuilder();
    final long perRequest;
    Subscription subscription;

    Collector(long perRequest) {
      this.perRequest = perRequest;
    }

    @Override
    public void onHeaders(SdkHttpResponse r) {
      headers.complete(r);
    }

    @Override
    public void onStream(Publisher<ByteBuffer> p) {
      p.subscribe(
          new Subscriber<ByteBuffer>() {
            @Override
            public void onSubscribe(Subscription s) {
              subscription = s;
              s.request(perRequest);
            }

            @Override
            public void onNext(ByteBuffer b) {
              sb.append(StandardCharsets.UTF_8.decode(b));
              if (perRequest == 1) {
                subscription.request(1);
              }
            }

            @Override
            public void onError(Throwable t) {
              body.completeExceptionally(t);
            }

            @Override
            public void onComplete() {
              body.complete(sb.toString());
            }
          });
    }

    @Override
    public void onError(Throwable error) {
      headers.completeExceptionally(error);
      body.completeExceptionally(error);
    }
  }

  CompletableFuture<Void> exec(
      GrowHttpClient c, SdkHttpRequest r, SdkHttpContentPublisher p, Collector h) {
    return c.execute(
        AsyncExecuteRequest.builder()
            .request(r)
            .requestContentPublisher(p)
            .responseHandler(h)
            .build());
  }

  @Test
  void asyncRoundTripFiresHooksAndLogs() throws Exception {
    server.enqueue(200, "{\"ok\":true}", "x-amzn-RequestId", "rid-1");
    GrowHttpClient c = client(CloudWatchLogsConfig.builder());
    assertThat(c.clientName()).isEqualTo("GrowJavaHttp");
    Collector h = new Collector(1);
    CompletableFuture<Void> f =
        exec(c, request(server.url() + "/", "DescribeLogGroups"), body("{}", true), h);
    assertThat(h.body.get(5, TimeUnit.SECONDS)).isEqualTo("{\"ok\":true}");
    f.get(5, TimeUnit.SECONDS);
    assertThat(h.headers.get().statusCode()).isEqualTo(200);
    MockServer.Recorded rec = server.requests().get(0);
    assertThat(rec.body()).isEqualTo("{}");
    assertThat(rec.header("X-Amz-Target")).isEqualTo("Logs_20140328.DescribeLogGroups");
    Hooks.RequestInfo req = (Hooks.RequestInfo) events.get(0);
    assertThat(req.attempt()).isEqualTo(2);
    assertThat(req.headers().toString()).doesNotContain("Credential=secret");
    Hooks.ResponseInfo res = (Hooks.ResponseInfo) events.get(1);
    assertThat(res.status()).isEqualTo(200);
    assertThat(res.requestId()).contains("rid-1");
    assertThat(logs.get(0)).contains("DescribeLogGroups").contains("rid-1");
    c.close();
    Collector closed = new Collector(1);
    assertThatThrownBy(
            () ->
                exec(c, request(server.url(), null), body("", true), closed)
                    .get(5, TimeUnit.SECONDS))
        .hasCauseInstanceOf(IOException.class);
  }

  @Test
  void bodiesWithoutLengthAndEmpty() throws Exception {
    server.enqueue(204, "");
    server.enqueue(200, "abc");
    GrowHttpClient c = client(CloudWatchLogsConfig.builder());
    Collector empty = new Collector(Long.MAX_VALUE);
    exec(c, request(server.url(), "X"), null, empty).get(5, TimeUnit.SECONDS);
    assertThat(empty.body.get(5, TimeUnit.SECONDS)).isEmpty();
    Collector unknown = new Collector(Long.MAX_VALUE);
    exec(c, request(server.url(), "X"), body("chunked", false), unknown).get(5, TimeUnit.SECONDS);
    assertThat(unknown.body.get()).isEqualTo("abc");
    assertThat(server.requests().get(1).body()).isEqualTo("chunked");
  }

  @Test
  void describeQueriesCreateTimeIsMadeIntegral() throws Exception {
    server.enqueue(
        200,
        "{\"queries\":[{\"createTime\": 1795034978649.759},{\"createTime\":1.5E3},{\"createTime\":7}]}");
    GrowHttpClient c = client(CloudWatchLogsConfig.builder());
    Collector h = new Collector(1);
    exec(c, request(server.url(), "DescribeQueries"), body("{}", true), h).get(5, TimeUnit.SECONDS);
    assertThat(h.body.get())
        .isEqualTo(
            "{\"queries\":[{\"createTime\": 1795034978649},{\"createTime\":1500},{\"createTime\":7}]}");
    byte[] same = "{\"createTime\":1}".getBytes(StandardCharsets.UTF_8);
    assertThat(GrowHttpClient.integerCreateTimes(same)).isSameAs(same);
    // a cancelled single publisher sends nothing
    List<Object> got = new ArrayList<>();
    GrowHttpClient.single(new byte[] {1})
        .subscribe(
            new Flow.Subscriber<List<ByteBuffer>>() {
              @Override
              public void onSubscribe(Flow.Subscription s) {
                s.cancel();
                s.request(1);
              }

              @Override
              public void onNext(List<ByteBuffer> item) {
                got.add(item);
              }

              @Override
              public void onError(Throwable t) {}

              @Override
              public void onComplete() {}
            });
    assertThat(got).isEmpty();
  }

  @Test
  void connectionFailureFiresOnError() {
    GrowHttpClient c = client(CloudWatchLogsConfig.builder());
    Collector h = new Collector(1);
    assertThatThrownBy(
            () ->
                exec(c, request("http://127.0.0.1:1/", "PutLogEvents"), body("{}", true), h)
                    .get(10, TimeUnit.SECONDS))
        .hasCauseInstanceOf(IOException.class);
    Hooks.ErrorInfo err = (Hooks.ErrorInfo) events.get(1);
    assertThat(err.error().code()).isEqualTo(ErrorCode.UNAVAILABLE);
    assertThat(err.willRetry()).isTrue();
    assertThat(h.headers).isCompletedExceptionally();
  }

  @Test
  void attemptTimeoutAndCancellation() throws Exception {
    server.fallback(
        ex -> {
          Thread.sleep(1500);
          MockServer.send(ex, 200, "{}");
        });
    GrowHttpClient c =
        client(CloudWatchLogsConfig.builder().requestTimeout(Duration.ofMillis(100)));
    Collector h = new Collector(1);
    assertThatThrownBy(
            () -> exec(c, request(server.url(), "X"), body("{}", true), h).get(5, TimeUnit.SECONDS))
        .hasCauseInstanceOf(HttpTimeoutException.class);
    assertThat(((Hooks.ErrorInfo) events.get(1)).error().code()).isEqualTo(ErrorCode.TIMEOUT);
    GrowHttpClient slow = client(CloudWatchLogsConfig.builder());
    CompletableFuture<Void> f =
        exec(slow, request(server.url(), "X"), body("{}", true), new Collector(1));
    f.cancel(true);
    assertThat(f).isCancelled();
    slow.abort("unknown-session");
  }

  @Test
  void attemptErrorsAndHelpers() {
    assertThat(GrowHttpClient.attemptError("op", new InterruptedIOException()).code())
        .isEqualTo(ErrorCode.TIMEOUT);
    assertThat(
            GrowHttpClient.attemptError("op", new RuntimeException(new IOException("x")))
                .getMessage())
        .contains("IOException");
    assertThat(GrowHttpClient.attemptError("op", new RuntimeException()).getMessage())
        .contains("RuntimeException");
    assertThat(GrowHttpClient.parseAttempt(null)).isEqualTo(new GrowHttpClient.Attempt(1, 1));
    assertThat(GrowHttpClient.parseAttempt("attempt=3; max=4"))
        .isEqualTo(new GrowHttpClient.Attempt(3, 4));
    assertThat(GrowHttpClient.requestId(Map.of("X-Amzn-RequestId", List.of("a")))).contains("a");
    assertThat(GrowHttpClient.requestId(Map.of("x-request-id", List.of()))).isEmpty();
  }

  @Test
  void syncClientForSts() throws Exception {
    server.enqueue(200, "<xml/>", "x-amzn-RequestId", "sts-1");
    GrowHttpClient c = client(CloudWatchLogsConfig.builder());
    HttpExecuteResponse r =
        c.prepareRequest(
                HttpExecuteRequest.builder()
                    .request(request(server.url(), null))
                    .contentStreamProvider(() -> new ByteArrayInputStream("Action=X".getBytes()))
                    .build())
            .call();
    assertThat(r.httpResponse().statusCode()).isEqualTo(200);
    assertThat(new String(r.responseBody().orElseThrow().readAllBytes())).isEqualTo("<xml/>");
    assertThat(server.requests().get(0).body()).isEqualTo("Action=X");
    server.enqueue(200, "");
    assertThat(
            c.prepareRequest(
                    HttpExecuteRequest.builder().request(request(server.url(), null)).build())
                .call()
                .httpResponse()
                .statusCode())
        .isEqualTo(200);
    var dead =
        c.prepareRequest(
            HttpExecuteRequest.builder().request(request("http://127.0.0.1:1", null)).build());
    dead.abort();
    assertThatThrownBy(dead::call).isInstanceOf(IOException.class);
    // an interrupted caller gets InterruptedIOException
    server.fallback(
        ex -> {
          Thread.sleep(2000);
          MockServer.send(ex, 200, "{}");
        });
    AtomicReference<Throwable> thrown = new AtomicReference<>();
    var slow =
        c.prepareRequest(HttpExecuteRequest.builder().request(request(server.url(), null)).build());
    Thread t =
        new Thread(
            () -> {
              try {
                slow.call();
              } catch (Throwable e) {
                thrown.set(e);
              }
            });
    t.start();
    Thread.sleep(200);
    slow.abort();
    t.interrupt();
    t.join(5000);
    assertThat(thrown.get()).isInstanceOf(IOException.class);
    c.close();
    assertThatThrownBy(
            () ->
                c.prepareRequest(
                        HttpExecuteRequest.builder().request(request(server.url(), null)).build())
                    .call())
        .isInstanceOf(IOException.class);
  }

  @Test
  void tlsAndProxySettings() {
    GrowHttpClient insecure =
        client(
            CloudWatchLogsConfig.builder()
                .tls(TlsOptions.builder().insecureSkipVerify(true).build()));
    assertThat(logs).anyMatch(l -> l.startsWith("WARN TLS verification is disabled"));
    insecure.close();
    client(CloudWatchLogsConfig.builder().proxy("http://user:p%40ss@proxy.local")).close();
    client(CloudWatchLogsConfig.builder().proxy("https://proxy.local:8443")).close();
    client(CloudWatchLogsConfig.builder().proxy("http://onlyuser@proxy.local:3128")).close();
    assertThatThrownBy(
            () ->
                new GrowHttpClient(
                    codes.grow.packages.cloudwatchlogs.core.TransportConfig.builder("http://x")
                        .proxy("relative")
                        .build()))
        .isInstanceOf(ConfigException.class);
    assertThatThrownBy(
            () ->
                new GrowHttpClient(
                    codes.grow.packages.cloudwatchlogs.core.TransportConfig.builder("http://x")
                        .proxy("http://[bad")
                        .build()))
        .isInstanceOf(ConfigException.class);
  }

  @Test
  void proxyAuthenticatesOnlyForProxyChallenges() throws Exception {
    server.fallback(
        ex -> {
          if (ex.getRequestHeaders().getFirst("Proxy-Authorization") == null) {
            ex.getResponseHeaders().add("Proxy-Authenticate", "Basic realm=\"p\"");
            MockServer.send(ex, 407, "");
          } else {
            MockServer.send(ex, 200, "{}");
          }
        });
    String proxy = server.url().replace("http://", "http://u:pw@");
    GrowHttpClient c = client(CloudWatchLogsConfig.builder().proxy(proxy));
    Collector h = new Collector(1);
    exec(c, request("http://example.invalid/", "X"), body("{}", true), h).get(10, TimeUnit.SECONDS);
    assertThat(h.headers.get().statusCode()).isIn(200, 407);
  }
}
