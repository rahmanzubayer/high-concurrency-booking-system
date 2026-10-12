import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * End-to-end flash sale load test.
 * <p>
 * Fires N simultaneous booking requests from N distinct users at a single item and
 * reports the status code distribution, latency percentiles and throughput.
 * <p>
 * Every request is issued from its own virtual thread, and all of them block on a
 * single starting gate, so the traffic arrives as a genuine thundering herd rather
 * than a ramp. This is the "10,000 users at exactly 12:00 PM" scenario.
 * <p>
 * Requires no dependencies. Run with the Java 21+ single-file launcher:
 * <p>
 * java FlashSaleLoadTest.java
 * java -Dusers=10000 -Dtarget=http://localhost:8080 FlashSaleLoadTest.java
 */
public class FlashSaleLoadTest {

    static final String TARGET = System.getProperty("target", "http://localhost:8080");
    static final String ADMIN = System.getProperty("admin", "http://localhost:8081");
    static final String ITEM = System.getProperty("item", "limited-item-01");
    static final int USERS = Integer.getInteger("users", 10_000);

    public static void main(String[] args) throws Exception {

        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        System.out.printf("target=%s  item=%s  users=%d%n%n", TARGET, ITEM, USERS);

        // --- 1. Reset the sale: stock back to total_stock, booked_users cleared ---
        HttpResponse<String> warm = client.send(
                HttpRequest.newBuilder(URI.create(ADMIN + "/api/v1/admin/sales/" + ITEM + "/warmup"))
                        .POST(HttpRequest.BodyPublishers.noBody())
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        System.out.println("warm-up -> HTTP " + warm.statusCode() + "  " + warm.body());
        if (warm.statusCode() != 200) {
            System.out.println("Warm-up failed. Aborting.");
            return;
        }

        // --- 2. Arm one virtual thread per user, all waiting on the same gate ---
        Map<Integer, AtomicInteger> codes = new ConcurrentHashMap<>();
        List<Long> latencies = Collections.synchronizedList(new ArrayList<>(USERS));
        AtomicInteger errors = new AtomicInteger();

        CountDownLatch armed = new CountDownLatch(USERS);
        CountDownLatch fire = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(USERS);

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {

            for (int i = 0; i < USERS; i++) {
                final String userId = "load_user_" + i;

                pool.submit(() -> {
                    armed.countDown();
                    try {
                        fire.await();

                        long t0 = System.nanoTime();
                        HttpResponse<String> res = client.send(
                                HttpRequest.newBuilder(
                                                URI.create(TARGET + "/api/v1/bookings/items/" + ITEM))
                                        .header("X-User-Id", userId)
                                        .timeout(Duration.ofSeconds(30))
                                        .POST(HttpRequest.BodyPublishers.noBody())
                                        .build(),
                                HttpResponse.BodyHandlers.ofString());
                        long ms = (System.nanoTime() - t0) / 1_000_000;

                        latencies.add(ms);
                        codes.computeIfAbsent(res.statusCode(), k -> new AtomicInteger())
                                .incrementAndGet();

                    } catch (Exception e) {
                        errors.incrementAndGet();
                    } finally {
                        done.countDown();
                    }
                });
            }

            armed.await();
            System.out.printf("%d virtual threads armed. Firing...%n", USERS);

            long start = System.nanoTime();
            fire.countDown();
            done.await();
            long wallMs = (System.nanoTime() - start) / 1_000_000;

            report(codes, latencies, errors.get(), wallMs);
        }
    }

    static void report(Map<Integer, AtomicInteger> codes, List<Long> latencies,
                       int errors, long wallMs) {

        System.out.println("\n================ RESULT ================");

        int total = errors;
        for (Map.Entry<Integer, AtomicInteger> e : new TreeMap<>(codes).entrySet()) {
            System.out.printf("  HTTP %-3d  %6d   %s%n",
                    e.getKey(), e.getValue().get(), label(e.getKey()));
            total += e.getValue().get();
        }
        if (errors > 0) {
            System.out.printf("  %-9s %6d   client-side failures (timeout, refused, reset)%n",
                    "ERROR", errors);
        }

        List<Long> sorted = new ArrayList<>(latencies);
        Collections.sort(sorted);
        if (!sorted.isEmpty()) {
            System.out.printf("%n  latency   p50 %d ms   p95 %d ms   p99 %d ms   max %d ms%n",
                    sorted.get(sorted.size() * 50 / 100),
                    sorted.get(sorted.size() * 95 / 100),
                    sorted.get(sorted.size() * 99 / 100),
                    sorted.get(sorted.size() - 1));
        }

        System.out.printf("  wall      %d ms for %d requests  (%.0f req/s)%n",
                wallMs, total, total * 1000.0 / Math.max(wallMs, 1));
        System.out.println("========================================");
        System.out.println("\nNow verify the invariants against Redis and PostgreSQL.");
    }

    static String label(int code) {
        return switch (code) {
            case 202 -> "slot secured";
            case 409 -> "duplicate user";
            case 410 -> "sold out";
            case 429 -> "shed by rate limiter";
            case 503 -> "degraded / busy";
            default -> "";
        };
    }
}
