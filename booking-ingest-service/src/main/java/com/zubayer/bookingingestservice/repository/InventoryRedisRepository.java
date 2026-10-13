package com.zubayer.bookingingestservice.repository;

import lombok.extern.slf4j.Slf4j;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import org.springframework.dao.QueryTimeoutException;
import com.zubayer.bookingingestservice.dto.AcquireResult;
import com.zubayer.bookingingestservice.dto.AcquireStatus;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.dao.DataAccessResourceFailureException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;

import java.util.List;
import java.util.UUID;
import java.time.Duration;
import java.nio.charset.StandardCharsets;

// Local Imports
import com.zubayer.bookingingestservice.redis.RedisKeys;

@Slf4j
@Repository
@RequiredArgsConstructor
public class InventoryRedisRepository {

    private static final long KEY_TTL_SECONDS = Duration.ofHours(24).toSeconds();

    private final StringRedisTemplate redisTemplate;
    private final RedisScript<Long> decrementScript;

    @CircuitBreaker(name = "redisInventory", fallbackMethod = "acquireFallback")

    public AcquireResult tryAcquireSlot(String itemId, String userId) {

        String transactionId = transactionId(itemId, userId);

        String inventoryKey = RedisKeys.inventory(itemId);
        String bookedUsersKey = RedisKeys.bookedUsers(itemId);
        String outboxKey = RedisKeys.outbox(itemId);
        String soldOutKey = RedisKeys.soldOut(itemId);

        Long code = redisTemplate.execute(
                decrementScript,
                List.of(inventoryKey, bookedUsersKey, outboxKey, soldOutKey),
                userId,
                itemId,
                transactionId,
                String.valueOf(System.currentTimeMillis()),
                String.valueOf(KEY_TTL_SECONDS)
        );

        return new AcquireResult(AcquireStatus.from(code), transactionId);
    }

    /**
     * Deterministic per (item, user). A user can hold at most one slot per item, so
     * this pair uniquely identifies the booking — which means a client retry after a
     * timeout produces the SAME id, and the worker's unique constraint recognizes it.
     * A random UUID would have produced a new id and a second order.
     */
    public static String transactionId(String itemId, String userId) {
        return UUID.nameUUIDFromBytes((itemId + '#' + userId).getBytes(StandardCharsets.UTF_8)).toString();
    }

    // --- Fallbacks -------------------------------------------------------------
    // Resilience4j selects a fallback by the thrown type. Anything not matched here
    // propagates normally — so a bug in the Lua script still fails loudly instead of
    // being quietly dressed up as a temporary outage.
    public AcquireResult acquireFallback(String itemId, String userId, DataAccessResourceFailureException exp) {
        return degraded(itemId, userId, "redis unreachable", exp);
    }

    public AcquireResult acquireFallback(String itemId, String userId, QueryTimeoutException exp) {
        return degraded(itemId, userId, "redis timed out", exp);
    }

    public AcquireResult acquireFallback(String itemId, String userId, CallNotPermittedException exp) {
        return degraded(itemId, userId, "circuit open, failing fast", exp);
    }

    private AcquireResult degraded(String itemId, String userId, String reason, Throwable t) {
        log.warn("Inventory degraded for item {} ({}): {}", itemId, reason, t.toString());
        return new AcquireResult(AcquireStatus.UNKNOWN, transactionId(itemId, userId));
    }

}
