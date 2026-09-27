package akt;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class App {
  static final Pattern MARKER = Pattern.compile("\\{\\s*\"akt\"\\s*:\\s*\\{\\s*\"run\"\\s*:\\s*\"([^\"]*)\"\\s*,\\s*\"seq\"\\s*:\\s*(\\d+)");
  static final int EXIT_IDLE = 3;

  record Phase(int messages, double ratePerS) {}

  record Config(Map<String, String> env) {
    String get(String k) {
      String v = env.get(k);
      if (v == null || v.isEmpty()) throw new IllegalArgumentException("missing env " + k);
      return v;
    }

    String get(String k, String def) {
      String v = env.get(k);
      return v == null || v.isEmpty() ? def : v;
    }

    List<Phase> phases() {
      List<Phase> out = new ArrayList<>();
      for (String p : get("AKT_PHASES").split(",")) {
        String[] mr = p.trim().split("@");
        out.add(new Phase(Integer.parseInt(mr[0]), Double.parseDouble(mr[1])));
      }
      return out;
    }
  }

  /** One ledger row, serialized in the LedgerRow JSON shape. */
  record Row(Config c, String side, long seq, String destination, int partition, long offset,
      SpanContext span, long tsNs, boolean markerInHeaders) {
    String json() {
      StringBuilder b = new StringBuilder(384).append('{');
      str(b, "run_id", c.get("AKT_RUN_ID")).append(',');
      str(b, "scenario_id", c.get("AKT_SCENARIO_ID")).append(',');
      b.append("\"seq\":").append(seq).append(',');
      str(b, "side", side).append(',');
      str(b, "service", c.get("AKT_SERVICE")).append(',');
      str(b, "language", "java").append(',');
      str(b, "tracer", c.get("AKT_TRACER", "none")).append(',');
      str(b, "tracer_version", c.get("AKT_TRACER_VERSION", "")).append(',');
      str(b, "broker", c.get("AKT_BROKER", "kafka")).append(',');
      str(b, "destination", destination).append(',');
      b.append("\"native_id\":{\"partition\":").append(partition).append(",\"offset\":").append(offset).append("},");
      str(b, "trace_id", span.getTraceId()).append(',');
      str(b, "span_id", span.getSpanId()).append(',');
      b.append("\"ts_ns\":").append(tsNs);
      if (markerInHeaders) b.append(",\"marker_in_headers\":true");
      return b.append('}').toString();
    }

    private static StringBuilder str(StringBuilder b, String k, String v) {
      b.append('"').append(k).append("\":\"");
      for (char ch : v.toCharArray()) {
        if (ch == '"' || ch == '\\') b.append('\\').append(ch);
        else if (ch < 0x20) b.append(String.format("\\u%04x", (int) ch));
        else b.append(ch);
      }
      return b.append('"');
    }
  }

  static long nowNs() {
    Instant t = Instant.now();
    return t.getEpochSecond() * 1_000_000_000L + t.getNano();
  }

  public static void main(String[] args) throws Exception {
    Config c = new Config(System.getenv());
    LedgerClient ledger = new LedgerClient(URI.create(c.get("AKT_LEDGER_URL")), 100_000);
    int code = switch (c.get("AKT_ROLE")) {
      case "producer" -> produce(c, ledger);
      case "consumer" -> consume(c, ledger);
      default -> throw new IllegalArgumentException("unsupported AKT_ROLE " + c.get("AKT_ROLE"));
    };
    boolean ok = ledger.close(Duration.ofSeconds(30));
    System.out.printf("ledger delivered=%d dropped=%d drained=%b%n", ledger.delivered(), ledger.dropped(), ok);
    Thread.sleep(Long.parseLong(c.get("AKT_EXIT_GRACE_MS", "3000")));
    System.exit(ok ? code : 1);
  }

  static Tracer tracer() { return GlobalOpenTelemetry.getTracer("akt"); }

  static int produce(Config c, LedgerClient ledger) throws Exception {
    String topic = c.get("AKT_TOPIC"), run = c.get("AKT_RUN_ID");
    long base = Long.parseLong(c.get("AKT_SEQ_BASE", "0"));
    Properties p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, c.get("AKT_BOOTSTRAP"));
    p.put(ProducerConfig.ACKS_CONFIG, "all");
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    Tracer tracer = tracer();
    AtomicLong failed = new AtomicLong();
    long n = 0;
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
      for (Phase ph : c.phases()) {
        long start = System.nanoTime();
        for (int k = 0; k < ph.messages(); k++) {
          long due = start + (long) (k * 1e9 / ph.ratePerS());
          long wait = due - System.nanoTime();
          if (wait > 0) Thread.sleep(wait / 1_000_000, (int) (wait % 1_000_000));
          long seq = base + ++n;
          String body = "{\"akt\":{\"run\":\"" + run + "\",\"seq\":" + seq + "},\"data\":{\"n\":" + n + ",\"item\":\"widget-" + (n % 97) + "\",\"qty\":" + (n % 7 + 1) + "}}";
          Span span = tracer.spanBuilder("akt.send").startSpan();
          try (Scope ignored = span.makeCurrent()) {
            SpanContext sc = span.getSpanContext();
            long ts = nowNs();
            producer.send(new ProducerRecord<>(topic, body), (md, ex) -> {
              if (ex != null) {
                failed.incrementAndGet();
                System.err.println("send seq " + seq + " failed: " + ex);
                return;
              }
              ledger.offer(new Row(c, "producer", seq, topic, md.partition(), md.offset(), sc, ts, false).json());
            });
          } finally {
            span.end();
          }
        }
      }
      producer.flush();
    }
    System.out.printf("produced %d messages to %s, %d failed%n", n, topic, failed.get());
    return failed.get() == 0 ? 0 : 1;
  }

  static boolean headersCarryMarker(Iterable<Header> headers) {
    for (Header h : headers) {
      if (h.key().contains("akt")) return true;
      if (h.value() != null && new String(h.value(), StandardCharsets.UTF_8).contains("akt")) return true;
    }
    return false;
  }

  static int consume(Config c, LedgerClient ledger) {
    String topic = c.get("AKT_TOPIC"), run = c.get("AKT_RUN_ID");
    int expect = Integer.parseInt(c.get("AKT_EXPECT"));
    Duration idle = Duration.ofSeconds(Long.parseLong(c.get("AKT_IDLE_TIMEOUT_S", "60")));
    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, c.get("AKT_BOOTSTRAP"));
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "akt-" + c.get("AKT_SERVICE"));
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    Set<Long> seen = new HashSet<>();
    long lastRecord = System.nanoTime(), records = 0;
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
      consumer.subscribe(List.of(topic));
      while (seen.size() < expect) {
        if (System.nanoTime() - lastRecord > idle.toNanos()) {
          System.err.printf("idle for %s with %d/%d seqs seen%n", idle, seen.size(), expect);
          return EXIT_IDLE;
        }
        for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
          SpanContext sc = Span.current().getSpanContext();
          long ts = nowNs();
          lastRecord = System.nanoTime();
          records++;
          Matcher m = r.value() == null ? null : MARKER.matcher(r.value());
          if (m == null || !m.find() || !m.group(1).equals(run)) {
            System.err.printf("record %s-%d@%d has no marker for run %s%n", r.topic(), r.partition(), r.offset(), run);
            continue;
          }
          long seq = Long.parseLong(m.group(2));
          seen.add(seq);
          ledger.offer(new Row(c, "consumer", seq, r.topic(), r.partition(), r.offset(), sc, ts, headersCarryMarker(r.headers())).json());
        }
      }
    }
    System.out.printf("consumed %d records, %d distinct seqs from %s%n", records, seen.size(), topic);
    return 0;
  }
}
