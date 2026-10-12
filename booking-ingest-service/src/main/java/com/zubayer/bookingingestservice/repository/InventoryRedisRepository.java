package com.zubayer.bookingingestservice.repository;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import com.zubayer.bookingingestservice.dto.AcquireResult;
import com.zubayer.bookingingestservice.dto.AcquireStatus;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;
import java.util.UUID;
import java.nio.charset.StandardCharsets;

// Local Imports
import com.zubayer.bookingingestservice.redis.RedisKeys;

@Repository
@RequiredArgsConstructor
public class InventoryRedisRepository {

    private final StringRedisTemplate redisTemplate;
    private final RedisScript<Long> decrementScript;

    public AcquireResult tryAcquireSlot(String itemId, String userId) {

        String transactionId = transactionId(itemId, userId);

        String inventoryKey = RedisKeys.inventory(itemId);
        String bookedUsersKey = RedisKeys.bookedUsers(itemId);

        Long code = redisTemplate.execute(
                decrementScript,
                List.of(inventoryKey, bookedUsersKey),
                userId
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


}
