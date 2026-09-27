package akt;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Asynchronous ledger client: offer() never blocks; a sender thread posts batches with retries. */
public final class LedgerClient implements AutoCloseable {
  private static final int MAX_BATCH = 500;
  private static final long MIN_BACKOFF_MS = 100, MAX_BACKOFF_MS = 1000;

  private final URI url;
  private final BlockingQueue<String> queue;
  private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
  private final AtomicLong dropped = new AtomicLong(), delivered = new AtomicLong();
  private final Thread sender;
  private volatile boolean closing;
  private volatile long deadlineNanos = Long.MAX_VALUE;

  public LedgerClient(URI url, int capacity) {
    this.url = url.resolve("/v1/rows");
    this.queue = new ArrayBlockingQueue<>(capacity);
    this.sender = new Thread(this::run, "akt-ledger");
    sender.setDaemon(true);
    sender.start();
  }

  public boolean offer(String rowJson) {
    if (queue.offer(rowJson)) return true;
    dropped.incrementAndGet();
    return false;
  }

  public long dropped() { return dropped.get(); }

  public long delivered() { return delivered.get(); }

  /** Drains the buffer, retrying until delivered or the timeout passes; true when nothing was lost. */
  public boolean close(Duration timeout) throws InterruptedException {
    deadlineNanos = System.nanoTime() + timeout.toNanos();
    closing = true;
    sender.join(Math.max(1, timeout.toMillis()));
    return !sender.isAlive() && queue.isEmpty() && dropped.get() == 0;
  }

  @Override
  public void close() throws InterruptedException { close(Duration.ofSeconds(30)); }

  private void run() {
    List<String> batch = new ArrayList<>();
    try {
      while (true) {
        if (batch.isEmpty()) {
          String first = queue.poll(100, TimeUnit.MILLISECONDS);
          if (first == null) {
            if (closing) return;
            continue;
          }
          batch.add(first);
          queue.drainTo(batch, MAX_BATCH - 1);
        }
        long backoff = MIN_BACKOFF_MS;
        int code;
        while ((code = post(batch)) / 100 != 2 && code != 400) {
          if (closing && System.nanoTime() > deadlineNanos) return;
          Thread.sleep(backoff);
          backoff = Math.min(backoff * 2, MAX_BACKOFF_MS);
        }
        if (code == 400) {
          System.err.println("ledger: collector rejected a batch of " + batch.size() + " rows (400)");
          dropped.addAndGet(batch.size());
        } else {
          delivered.addAndGet(batch.size());
        }
        batch.clear();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private int post(List<String> batch) {
    HttpRequest req = HttpRequest.newBuilder(url)
        .timeout(Duration.ofSeconds(5))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString("[" + String.join(",", batch) + "]"))
        .build();
    try {
      return http.send(req, HttpResponse.BodyHandlers.discarding()).statusCode();
    } catch (Exception e) {
      if (e instanceof InterruptedException) Thread.currentThread().interrupt();
      return 0;
    }
  }
}
