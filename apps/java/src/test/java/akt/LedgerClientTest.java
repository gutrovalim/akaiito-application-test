package akt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class LedgerClientTest {
  private static final Pattern SEQ = Pattern.compile("\"seq\":(\\d+)");

  @Test
  void nonBlockingWithRetries() throws Exception {
    long downUntil = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    Set<Long> received = ConcurrentHashMap.newKeySet();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/v1/rows", ex -> {
      String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
      if (System.nanoTime() < downUntil) {
        ex.sendResponseHeaders(503, -1);
      } else {
        Matcher m = SEQ.matcher(body);
        while (m.find()) received.add(Long.parseLong(m.group(1)));
        ex.sendResponseHeaders(204, -1);
      }
      ex.close();
    });
    server.start();
    try {
      LedgerClient client = new LedgerClient(URI.create("http://127.0.0.1:" + server.getAddress().getPort()), 1000);
      long start = System.nanoTime();
      for (int seq = 1; seq <= 100; seq++) {
        assertTrue(client.offer("{\"seq\":" + seq + ",\"side\":\"producer\"}"), "offer " + seq + " rejected");
      }
      long elapsedMs = (System.nanoTime() - start) / 1_000_000;
      assertTrue(elapsedMs < 1000, "enqueueing 100 rows took " + elapsedMs + " ms, want < 1000 while the collector is down");
      assertTrue(received.isEmpty(), "rows delivered while the collector returned 503");

      assertTrue(client.close(Duration.ofSeconds(20)), "client did not drain after the collector recovered");
      assertEquals(100, received.size(), "distinct rows delivered after recovery");
      for (long seq = 1; seq <= 100; seq++) assertTrue(received.contains(seq), "seq " + seq + " not delivered");
    } finally {
      server.stop(0);
    }
  }
}
