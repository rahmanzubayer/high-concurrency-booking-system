package com.zubayer.bookingingestservice.repository;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import org.springframework.context.annotation.Import;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import com.zubayer.bookingingestservice.redis.RedisKeys;
import com.zubayer.bookingingestservice.dto.AcquireResult;
import com.zubayer.bookingingestservice.dto.AcquireStatus;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import com.zubayer.bookingingestservice.config.RedisScriptConfig;
import org.springframework.boot.test.autoconfigure.data.redis.DataRedisTest;

import java.util.Map;
import java.util.List;
import java.util.EnumMap;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The invariants, asserted against a real Redis running the real Lua script.
 * <p>
 * Deliberately a narrow slice: only Redis is started, because the thing under test is the
 * atomicity of the inventory script. Kafka and PostgreSQL would add thirty seconds of
 * container startup to every CI run and could not make this assertion any stronger.
 *
 * @CircuitBreaker on the repository is inert here — the Resilience4j aspect is not part of
 * this slice — which is correct. These tests are about the script, not about degradation.
 */

@Testcontainers
@DataRedisTest
@Import({InventoryRedisRepository.class, RedisScriptConfig.class})
class InventoryRedisRepositoryIntegrationTest {

    private static final String ITEM = "test-item-01";
    private static final int STOCK = 100;
    private static final int USERS = 2_000;

    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine"))
                    .withExposedPorts(6379);

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    @Autowired
    private StringRedisTemplate redis;

    @Autowired
    private InventoryRedisRepository repository;

    @BeforeEach
    void resetSale() {
        redis.delete(List.of(
                RedisKeys.inventory(ITEM),
                RedisKeys.bookedUsers(ITEM),
                RedisKeys.outbox(ITEM),
                RedisKeys.soldOut(ITEM)));
    }

    private void warmUp(int stock) {
        redis.opsForValue().set(RedisKeys.inventory(ITEM), String.valueOf(stock));
    }

    @Test
    @DisplayName("Invariant 1: 2,000 concurrent buyers against 100 items sells exactly 100")
    void sellsExactlyTheAvailableStock() throws Exception {

        warmUp(STOCK);

        Map<AcquireStatus, AtomicInteger> outcomes = new EnumMap<>(AcquireStatus.class);
        for (AcquireStatus status : AcquireStatus.values()) {
            outcomes.put(status, new AtomicInteger());
        }

        CountDownLatch armed = new CountDownLatch(USERS);
        CountDownLatch fire = new CountDownLatch(1);

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < USERS; i++) {
                final String userId = "user_" + i;
                pool.submit(() -> {
                    armed.countDown();
                    fire.await();                                  // all threads start together
                    AcquireResult result = repository.tryAcquireSlot(ITEM, userId);
                    outcomes.get(result.status()).incrementAndGet();
                    return null;
                });
            }
            armed.await();
            fire.countDown();
        }   // close() awaits termination

        assertThat(outcomes.get(AcquireStatus.ACQUIRED).get())
                .as("exactly the available stock is sold, never more")
                .isEqualTo(STOCK);
        assertThat(outcomes.get(AcquireStatus.SOLD_OUT).get())
                .isEqualTo(USERS - STOCK);

        assertThat(redis.opsForValue().get(RedisKeys.inventory(ITEM)))
                .as("the counter lands on zero, never negative")
                .isEqualTo("0");
        assertThat(redis.opsForSet().size(RedisKeys.bookedUsers(ITEM)))
                .isEqualTo(STOCK);
        assertThat(redis.opsForStream().size(RedisKeys.outbox(ITEM)))
                .as("one outbox entry per slot granted — no slot without an event")
                .isEqualTo(STOCK);
        assertThat(redis.hasKey(RedisKeys.soldOut(ITEM)))
                .as("the edge flag is raised when the last slot goes")
                .isTrue();
    }

    @Test
    @DisplayName("Invariant 2: the same user hammering the endpoint gets exactly one slot")
    void grantsOneSlotPerUserUnderConcurrency() throws Exception {

        warmUp(STOCK);

        AtomicInteger acquired = new AtomicInteger();
        AtomicInteger duplicates = new AtomicInteger();

        CountDownLatch armed = new CountDownLatch(500);
        CountDownLatch fire = new CountDownLatch(1);

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 500; i++) {
                pool.submit(() -> {
                    armed.countDown();
                    fire.await();
                    AcquireResult result = repository.tryAcquireSlot(ITEM, "impatient_user");
                    if (result.status() == AcquireStatus.ACQUIRED) {
                        acquired.incrementAndGet();
                    } else if (result.status() == AcquireStatus.ALREADY_BOOKED) {
                        duplicates.incrementAndGet();
                    }
                    return null;
                });
            }
            armed.await();
            fire.countDown();
        }

        assertThat(acquired.get()).isEqualTo(1);
        assertThat(duplicates.get()).isEqualTo(499);
        assertThat(redis.opsForValue().get(RedisKeys.inventory(ITEM)))
                .as("499 rejected attempts consumed no inventory")
                .isEqualTo(String.valueOf(STOCK - 1));
        assertThat(redis.opsForStream().size(RedisKeys.outbox(ITEM)))
                .as("a rejected duplicate writes no event")
                .isEqualTo(1L);
    }

    @Test
    @DisplayName("An unwarmed item is NOT_AVAILABLE, not SOLD_OUT")
    void distinguishesUnwarmedFromSoldOut() {

        AcquireResult cold = repository.tryAcquireSlot(ITEM, "early_bird");
        assertThat(cold.status()).isEqualTo(AcquireStatus.NOT_AVAILABLE);

        warmUp(1);
        assertThat(repository.tryAcquireSlot(ITEM, "first").status())
                .isEqualTo(AcquireStatus.ACQUIRED);
        assertThat(repository.tryAcquireSlot(ITEM, "second").status())
                .isEqualTo(AcquireStatus.SOLD_OUT);
    }

    @Test
    @DisplayName("Transaction ids are deterministic per (item, user) across sales")
    void transactionIdIsStableAcrossSales() {

        warmUp(STOCK);
        String first = repository.tryAcquireSlot(ITEM, "alice").transactionId();

        resetSale();
        warmUp(STOCK);
        String second = repository.tryAcquireSlot(ITEM, "alice").transactionId();

        assertThat(second)
                .as("a retry must produce the same id, or the worker's unique "
                        + "constraint cannot recognise it as a duplicate")
                .isEqualTo(first);
    }
}
