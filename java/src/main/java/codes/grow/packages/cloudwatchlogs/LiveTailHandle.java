package codes.grow.packages.cloudwatchlogs;

import codes.grow.packages.cloudwatchlogs.core.CloudWatchLogsException;
import codes.grow.packages.cloudwatchlogs.logging.TailHandle;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.reactivestreams.Subscription;

/**
 * Handle of a live-tail session ({@code startLiveTail}). On AWS a session streams new events for up
 * to 3 hours; the Moto emulator replays matching existing events once and ends the session.
 */
public final class LiveTailHandle implements TailHandle {
  private final CompletableFuture<Void> done = new CompletableFuture<>();
  private final AtomicBoolean stopped = new AtomicBoolean();
  private final Runnable onEnd;
  private volatile String sessionId = "";
  private volatile CloudWatchLogsException error;
  private volatile Subscription subscription;
  private volatile CompletableFuture<?> call;

  LiveTailHandle(Runnable onEnd) {
    this.onEnd = onEnd;
  }

  void sessionId(String id) {
    this.sessionId = id == null ? "" : id;
  }

  void subscription(Subscription s) {
    this.subscription = s;
    if (stopped.get()) {
      s.cancel();
    }
  }

  void call(CompletableFuture<?> f) {
    this.call = f;
    if (stopped.get()) {
      f.cancel(true);
    }
  }

  boolean stoppedByCaller() {
    return stopped.get();
  }

  /** Ends the session; {@code failure} is recorded unless the caller stopped it. */
  void finish(CloudWatchLogsException failure) {
    if (done.isDone()) {
      return;
    }
    if (failure != null && !stopped.get()) {
      error = failure;
    }
    if (done.complete(null)) {
      try {
        onEnd.run();
      } catch (RuntimeException ignored) {
        // bookkeeping only
      }
    }
  }

  /**
   * Returns the session id from the {@code sessionStart} frame.
   *
   * @return the id (empty when the stream ended before the session started)
   */
  public String sessionId() {
    return sessionId;
  }

  /**
   * Returns a future that completes when the session ends (service closed it, error, or {@link
   * #close()}). It never completes exceptionally; see {@link #error()}.
   *
   * @return the future
   */
  public CompletableFuture<Void> done() {
    return done;
  }

  /**
   * Waits until the session ended or the timeout passed.
   *
   * @param timeout how long to wait
   * @return true when the session ended
   */
  public boolean await(Duration timeout) {
    try {
      done.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Returns the error that ended the session, if any (also passed to {@code onError}).
   *
   * @return the error, or empty
   */
  public Optional<CloudWatchLogsException> error() {
    return Optional.ofNullable(error);
  }

  /**
   * Returns whether the session has ended.
   *
   * @return true once ended
   */
  public boolean isClosed() {
    return done.isDone();
  }

  /** Stops the session (same as {@link #close()}). Idempotent. */
  @Override
  public void stop() {
    close();
  }

  /** Stops the session and releases the stream. Idempotent. */
  @Override
  public void close() {
    if (!stopped.compareAndSet(false, true)) {
      return;
    }
    // finish first: it aborts the HTTP exchange, which the SDK cancellation below does not do
    finish(null);
    Subscription s = subscription;
    if (s != null) {
      s.cancel();
    }
    CompletableFuture<?> f = call;
    if (f != null) {
      f.cancel(true);
    }
  }

  @Override
  public String toString() {
    return "LiveTailHandle{sessionId=" + sessionId + ", closed=" + isClosed() + "}";
  }
}
