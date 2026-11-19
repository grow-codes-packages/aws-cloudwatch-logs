package codes.grow.packages.cloudwatchlogs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import codes.grow.packages.cloudwatchlogs.core.CloudWatchLogsException;
import codes.grow.packages.cloudwatchlogs.core.PermissionDeniedException;
import codes.grow.packages.cloudwatchlogs.core.TimeoutException;
import codes.grow.packages.cloudwatchlogs.core.UnavailableException;
import codes.grow.packages.cloudwatchlogs.core.ValidationException;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LiveTailTest {
  static final String ARN = ClientTest.ARN;
  static final String START =
      "{\"requestId\":\"r\",\"sessionId\":\"sess-1\",\"logGroupIdentifiers\":[\"" + ARN + "\"]}";
  static final String UPDATE =
      "{\"sessionMetadata\":{\"sampled\":true},\"sessionResults\":[{\"logStreamName\":\"s1\","
          + "\"logGroupIdentifier\":\""
          + ARN
          + "\",\"message\":\"one\",\"timestamp\":1,\"ingestionTime\":2},"
          + "{\"message\":\"two\",\"timestamp\":3}]}";

  AwsMock mock;
  CloudWatchLogsClient client;

  @BeforeEach
  void start() throws Exception {
    mock = new AwsMock();
    client = mock.client(mock.config().accountId("123456789012"), i -> {});
  }

  @AfterEach
  void stop() {
    client.close();
    mock.close();
  }

  static LiveTailRequest group() {
    return LiveTailRequest.builder().groups(List.of(ARN)).build();
  }

  @Test
  void seedFixtureReplaysEvents() {
    mock.fixture("startLiveTail.seed");
    List<LiveTailEvent> events = Collections.synchronizedList(new ArrayList<>());
    List<LiveTailUpdate> updates = Collections.synchronizedList(new ArrayList<>());
    LiveTailHandle h =
        client.startLiveTail(
            LiveTailRequest.builder().groups(List.of("grow-bootstrap/seed")).pattern("x").build(),
            events::add,
            LiveTailOptions.builder().onUpdate(updates::add).build());
    assertThat(h.await(Duration.ofSeconds(5))).isTrue();
    assertThat(h.sessionId()).isEqualTo("dab2e97c-0f97-49b7-bc9d-d7a93d6341e3");
    assertThat(events).hasSize(3);
    assertThat(events.get(0).message()).contains("grow-seed-event");
    assertThat(events.get(0).logStreamName()).contains("seed-stream");
    assertThat(events.get(0).logGroupIdentifier()).isPresent();
    assertThat(events.get(0).ingestionTime()).isPresent();
    assertThat(updates).hasSize(1);
    assertThat(h.error()).isEmpty();
    assertThat(h.isClosed()).isTrue();
    assertThat(h.toString()).contains("dab2e97c");
    JsonNode body = mock.lastBody("StartLiveTail");
    assertThat(body.get("logGroupIdentifiers").get(0).asText())
        .isEqualTo("arn:aws:logs:us-east-1:123456789012:log-group:grow-bootstrap/seed");
    assertThat(body.get("logEventFilterPattern").asText()).isEqualTo("x");
  }

  @Test
  void decodedFixtureWithStreams() {
    mock.fixture("startLiveTail.ok");
    List<LiveTailEvent> events = Collections.synchronizedList(new ArrayList<>());
    LiveTailHandle h =
        client.startLiveTail(
            LiveTailRequest.builder().groups(List.of(ARN)).streams(List.of("s1")).build(),
            events::add);
    h.await(Duration.ofSeconds(5));
    assertThat(events).isNotEmpty();
    assertThat(mock.lastBody("StartLiveTail").get("logStreamNames").get(0).asText())
        .isEqualTo("s1");
    mock.fixture("startLiveTail.filter");
    LiveTailHandle p =
        client
            .startLiveTailAsync(
                LiveTailRequest.builder().groups(List.of(ARN)).streamPrefixes(List.of("s")).build(),
                e -> {
                  throw new IllegalStateException("callbacks never break the session");
                },
                LiveTailOptions.builder()
                    .onUpdate(
                        u -> {
                          throw new IllegalStateException("ignored");
                        })
                    .build())
            .join();
    p.await(Duration.ofSeconds(5));
    assertThat(mock.lastBody("StartLiveTail").get("logStreamNamePrefixes").get(0).asText())
        .isEqualTo("s");
  }

  /** Writes the frames, then keeps the stream open until the client goes away. */
  static codes.grow.packages.cloudwatchlogs.core.MockServer.Responder open(
      List<Map<String, Object>> frames, CountDownLatch closed) {
    return ex -> {
      ex.getResponseHeaders().add("Content-Type", "application/vnd.amazon.eventstream");
      ex.sendResponseHeaders(200, 0);
      OutputStream os = ex.getResponseBody();
      os.write(AwsMock.frames(frames));
      os.flush();
      try {
        for (int i = 0; i < 100; i++) {
          Thread.sleep(50);
          os.write(
              AwsMock.frames(
                  List.of(
                      AwsMock.frame(
                          "sessionUpdate",
                          "{\"sessionResults\":[],\"pad\":\"" + "x".repeat(65536) + "\"}"))));
          os.flush();
        }
      } catch (Exception e) {
        // client closed the stream
      } finally {
        closed.countDown();
      }
    };
  }

  @Test
  void openSessionStreamsUntilClosed() throws Exception {
    CountDownLatch closed = new CountDownLatch(1);
    mock.on(
        "StartLiveTail",
        open(
            List.of(
                AwsMock.frame("initial-response", "{}"),
                AwsMock.frame("sessionStart", START),
                AwsMock.frame("sessionUpdate", UPDATE),
                AwsMock.frame("sessionUpdate", "{\"sessionResults\":[]}")),
            closed));
    List<LiveTailEvent> events = Collections.synchronizedList(new ArrayList<>());
    List<LiveTailUpdate> updates = Collections.synchronizedList(new ArrayList<>());
    LiveTailHandle h =
        client.startLiveTail(
            group(), events::add, LiveTailOptions.builder().onUpdate(updates::add).build());
    assertThat(h.sessionId()).isEqualTo("sess-1");
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (updates.size() < 2 && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertThat(h.isClosed()).isFalse();
    assertThat(events).extracting(LiveTailEvent::message).containsExactly("one", "two");
    assertThat(updates.get(0).sampled()).isTrue();
    assertThat(updates.get(0).count()).isEqualTo(2);
    assertThat(updates.get(1).count()).isZero();
    h.stop();
    h.close();
    assertThat(h.done()).isDone();
    assertThat(h.error()).isEmpty();
  }

  @Test
  void clientCloseStopsSessions() throws Exception {
    CountDownLatch closed = new CountDownLatch(1);
    mock.on(
        "StartLiveTail",
        open(
            List.of(AwsMock.frame("initial-response", "{}"), AwsMock.frame("sessionStart", START)),
            closed));
    LiveTailHandle h = client.startLiveTail(group(), e -> {});
    client.close();
    assertThat(h.isClosed()).isTrue();
  }

  @Test
  void streamExceptionEndsTheSession() {
    List<CloudWatchLogsException> errors = Collections.synchronizedList(new ArrayList<>());
    mock.on(
        "StartLiveTail",
        ex ->
            AwsMock.sendBytes(
                ex,
                200,
                AwsMock.frames(
                    List.of(
                        AwsMock.frame("initial-response", "{}"),
                        AwsMock.frame("sessionStart", START),
                        AwsMock.frame("SessionStreamingException", "{\"message\":\"m\"}"))),
                "application/vnd.amazon.eventstream"));
    LiveTailHandle h =
        client.startLiveTail(
            group(),
            e -> {},
            LiveTailOptions.builder()
                .onError(
                    e -> {
                      errors.add(e);
                      throw new IllegalStateException("ignored");
                    })
                .build());
    assertThat(h.await(Duration.ofSeconds(5))).isTrue();
    assertThat(h.error()).containsInstanceOf(UnavailableException.class);
    assertThat(errors).hasSize(1);
    assertThat(errors.get(0).getMessage()).contains("SessionStreamingException");
  }

  @Test
  void exceptionBeforeTheSessionStartsFailsTheCall() {
    mock.on(
        "StartLiveTail",
        ex ->
            AwsMock.sendBytes(
                ex,
                200,
                AwsMock.frames(
                    List.of(
                        AwsMock.frame("initial-response", "{}"),
                        AwsMock.frame("SessionTimeoutException", "{\"message\":\"m\"}"))),
                "application/vnd.amazon.eventstream"));
    assertThatThrownBy(() -> client.startLiveTail(group(), e -> {}))
        .isInstanceOf(TimeoutException.class);
  }

  @Test
  void streamEndingBeforeSessionStart() {
    mock.on(
        "StartLiveTail",
        ex ->
            AwsMock.sendBytes(
                ex,
                200,
                AwsMock.frames(List.of(AwsMock.frame("initial-response", "{}"))),
                "application/vnd.amazon.eventstream"));
    LiveTailHandle h = client.startLiveTail(group(), e -> {});
    assertThat(h.sessionId()).isEmpty();
    assertThat(h.await(Duration.ofSeconds(5))).isTrue();
  }

  @Test
  void httpErrorsAndValidation() {
    mock.on("StartLiveTail", AwsMock.responder(AwsMock.load("ensureLogGroup.access-denied")));
    assertThatThrownBy(() -> client.startLiveTail(group(), e -> {}))
        .isInstanceOf(PermissionDeniedException.class);
    assertThatThrownBy(() -> client.startLiveTail(null, e -> {}))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(
            () ->
                client.startLiveTail(LiveTailRequest.builder().groups(List.of()).build(), e -> {}))
        .isInstanceOf(ValidationException.class);
    List<String> eleven = Collections.nCopies(11, ARN);
    assertThatThrownBy(
            () -> client.startLiveTail(LiveTailRequest.builder().groups(eleven).build(), e -> {}))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(
            () ->
                client.startLiveTail(
                    LiveTailRequest.builder()
                        .groups(List.of(ARN))
                        .streams(List.of("a"))
                        .streamPrefixes(List.of("b"))
                        .build(),
                    e -> {}))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(
            () ->
                client.startLiveTail(
                    LiveTailRequest.builder()
                        .groups(List.of(ARN, ARN))
                        .streams(List.of("a"))
                        .build(),
                    e -> {}))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(
            () ->
                client.startLiveTail(
                    LiveTailRequest.builder()
                        .groups(List.of(ARN, ARN))
                        .streamPrefixes(List.of("a"))
                        .build(),
                    e -> {}))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void sessionThatNeverStartsTimesOut() throws Exception {
    CountDownLatch closed = new CountDownLatch(1);
    mock.on("StartLiveTail", open(List.of(AwsMock.frame("initial-response", "{}")), closed));
    try (CloudWatchLogsClient c =
        mock.client(
            mock.config().accountId("123456789012").totalTimeout(Duration.ofMillis(300)),
            i -> {})) {
      assertThatThrownBy(() -> c.startLiveTail(group(), e -> {}))
          .isInstanceOf(TimeoutException.class);
    }
  }

  @Test
  void cancellingTheStartClosesTheSession() throws Exception {
    CountDownLatch closed = new CountDownLatch(1);
    mock.on("StartLiveTail", open(List.of(AwsMock.frame("initial-response", "{}")), closed));
    CompletableFuture<LiveTailHandle> f = client.startLiveTailAsync(group(), e -> {}, null);
    Thread.sleep(200);
    long t0 = System.nanoTime();
    f.cancel(true);
    assertThat(mock.requests("StartLiveTail").get(0).header(GrowHttpClient.SESSION_HEADER))
        .isNotNull();
    assertThat(closed.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(System.nanoTime() - t0).isLessThan(TimeUnit.SECONDS.toNanos(3));
  }
}
