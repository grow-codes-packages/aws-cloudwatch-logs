package codes.grow.packages.cloudwatchlogs;

import codes.grow.packages.cloudwatchlogs.core.MockServer;
import codes.grow.packages.cloudwatchlogs.core.RetryPolicy;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import software.amazon.awssdk.eventstream.HeaderValue;
import software.amazon.awssdk.eventstream.Message;

/** AWS JSON 1.1 mock on top of {@link MockServer}: routes by {@code X-Amz-Target}. */
final class AwsMock implements AutoCloseable {
  static final ObjectMapper JSON = new ObjectMapper();

  final MockServer server;
  private final Map<String, ConcurrentLinkedQueue<MockServer.Responder>> queues =
      new ConcurrentHashMap<>();
  private final Map<String, MockServer.Responder> defaults = new ConcurrentHashMap<>();

  AwsMock() throws IOException {
    server = MockServer.start();
    server.fallback(
        ex -> {
          String op = op(ex.getRequestHeaders().getFirst("X-Amz-Target"));
          ConcurrentLinkedQueue<MockServer.Responder> q = queues.get(op);
          MockServer.Responder r = q == null ? null : q.poll();
          if (r == null) {
            r = defaults.get(op);
          }
          if (r == null) {
            send(ex, 500, "<html>not mocked: " + op + "</html>", "text/html");
          } else {
            r.respond(ex);
          }
        });
  }

  static String op(String target) {
    if (target == null) {
      return "";
    }
    int dot = target.lastIndexOf('.');
    return dot >= 0 ? target.substring(dot + 1) : target;
  }

  /** Queues a responder for the next request of {@code op}. */
  AwsMock on(String op, MockServer.Responder r) {
    queues.computeIfAbsent(op, k -> new ConcurrentLinkedQueue<>()).add(r);
    return this;
  }

  /** Queues a JSON response. */
  AwsMock on(String op, int status, String body) {
    return on(op, ex -> send(ex, status, body, "application/x-amz-json-1.1"));
  }

  /** Queues an AWS error. */
  AwsMock error(String op, String code) {
    return on(
        op,
        ex -> {
          ex.getResponseHeaders().add("x-amzn-RequestId", "req-" + code);
          send(
              ex,
              400,
              "{\"__type\":\"" + code + "\",\"message\":\"m\"}",
              "application/x-amz-json-1.1");
        });
  }

  /** Answers every request of {@code op} with this responder when its queue is empty. */
  AwsMock always(String op, MockServer.Responder r) {
    defaults.put(op, r);
    return this;
  }

  /** Answers every request of {@code op} with this JSON body. */
  AwsMock always(String op, String body) {
    return always(op, ex -> send(ex, 200, body, "application/x-amz-json-1.1"));
  }

  /** Queues the recorded response of a fixture under its operation. */
  AwsMock fixture(String name) {
    JsonNode f = load(name);
    return on(f.get("operation").asText(), responder(f));
  }

  static JsonNode load(String name) {
    try (InputStream in = AwsMock.class.getResourceAsStream("/fixtures/" + name + ".json")) {
      if (in == null) {
        throw new IllegalArgumentException("no fixture " + name);
      }
      return JSON.readTree(in);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static MockServer.Responder responder(JsonNode f) {
    JsonNode res = f.get("response");
    int status = res.get("status").asInt();
    return ex -> {
      Iterator<Map.Entry<String, JsonNode>> it = res.path("headers").properties().iterator();
      String type = "application/x-amz-json-1.1";
      while (it.hasNext()) {
        Map.Entry<String, JsonNode> h = it.next();
        String k = h.getKey().toLowerCase();
        if (k.equals("content-length") || k.equals("status")) {
          continue;
        }
        if (k.equals("content-type")) {
          type = h.getValue().asText();
          continue;
        }
        ex.getResponseHeaders().add(h.getKey(), h.getValue().asText());
      }
      if (res.has("bodyBase64")) {
        sendBytes(ex, status, Base64.getDecoder().decode(res.get("bodyBase64").asText()), type);
        return;
      }
      if (res.has("eventStream")) {
        sendBytes(ex, status, eventStream(res.get("eventStream")), type);
        return;
      }
      JsonNode body = res.get("body");
      String text =
          body == null || body.isNull() ? "" : body.isTextual() ? body.asText() : body.toString();
      send(ex, status, text, type);
    };
  }

  /** Encodes decoded event-stream frames ({@code headers} + JSON {@code payload}). */
  static byte[] eventStream(JsonNode frames) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (JsonNode frame : frames) {
      Map<String, HeaderValue> headers = new LinkedHashMap<>();
      frame
          .get("headers")
          .properties()
          .forEach(h -> headers.put(h.getKey(), HeaderValue.fromString(h.getValue().asText())));
      JsonNode payload = frame.get("payload");
      byte[] bytes =
          payload == null || payload.isNull()
              ? new byte[0]
              : payload.toString().getBytes(StandardCharsets.UTF_8);
      byte[] encoded = new Message(headers, bytes).toByteBuffer().array();
      out.write(encoded, 0, encoded.length);
    }
    return out.toByteArray();
  }

  /** One event-stream frame. */
  static Map<String, Object> frame(String type, String payloadJson) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("type", type);
    m.put("payload", payloadJson);
    return m;
  }

  /**
   * Encodes frames given as (event-type, JSON payload); {@code !Exception} types become exceptions.
   */
  static byte[] frames(List<Map<String, Object>> frames) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (Map<String, Object> f : frames) {
      String type = (String) f.get("type");
      Map<String, HeaderValue> headers = new LinkedHashMap<>();
      if (type.endsWith("Exception")) {
        headers.put(":message-type", HeaderValue.fromString("exception"));
        headers.put(":exception-type", HeaderValue.fromString(type));
      } else {
        headers.put(":message-type", HeaderValue.fromString("event"));
        headers.put(":event-type", HeaderValue.fromString(type));
      }
      headers.put(":content-type", HeaderValue.fromString("application/json"));
      String payload = (String) f.get("payload");
      byte[] encoded =
          new Message(
                  headers, payload == null ? new byte[0] : payload.getBytes(StandardCharsets.UTF_8))
              .toByteBuffer()
              .array();
      out.write(encoded, 0, encoded.length);
    }
    return out.toByteArray();
  }

  static void send(HttpExchange ex, int status, String body, String type) throws IOException {
    sendBytes(ex, status, body.getBytes(StandardCharsets.UTF_8), type);
  }

  static void sendBytes(HttpExchange ex, int status, byte[] b, String type) throws IOException {
    ex.getResponseHeaders().add("Content-Type", type);
    ex.sendResponseHeaders(status, b.length == 0 ? -1 : b.length);
    if (b.length > 0) {
      try (OutputStream os = ex.getResponseBody()) {
        os.write(b);
      }
    }
  }

  /** Recorded requests of one operation. */
  List<MockServer.Recorded> requests(String op) {
    List<MockServer.Recorded> out = new ArrayList<>();
    for (MockServer.Recorded r : server.requests()) {
      if (op.equals(op(r.header("X-Amz-Target")))) {
        out.add(r);
      }
    }
    return out;
  }

  /** Body of the last request of {@code op} as JSON. */
  JsonNode lastBody(String op) {
    List<MockServer.Recorded> r = requests(op);
    try {
      return JSON.readTree(r.get(r.size() - 1).body());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  CloudWatchLogsConfig.Builder config() {
    return CloudWatchLogsConfig.builder()
        .url(server.url())
        .region("us-east-1")
        .accessKeyId("AKIDEXAMPLE")
        .secretAccessKey("secret-value-1234")
        .retry(RetryPolicy.none())
        .logGroup("g")
        .logStream("s");
  }

  CloudWatchLogsClient client() {
    return client(config(), c -> {});
  }

  CloudWatchLogsClient client(
      CloudWatchLogsConfig.Builder cfg, Consumer<CloudWatchLogsClient.Internals> change) {
    CloudWatchLogsClient.Internals in = new CloudWatchLogsClient.Internals();
    in.disableHostPrefix = true;
    in.sleeper = d -> CompletableFuture.completedFuture(null);
    change.accept(in);
    return CloudWatchLogsClient.create(cfg.build(), in);
  }

  static Duration ms(long v) {
    return Duration.ofMillis(v);
  }

  @Override
  public void close() {
    server.close();
  }
}
