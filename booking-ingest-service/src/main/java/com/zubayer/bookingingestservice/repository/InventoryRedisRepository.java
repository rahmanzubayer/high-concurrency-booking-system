package com.zubayer.bookingingestservice.repository;

import org.springframework.stereotype.Repository;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.List;

// Local Imports
import com.zubayer.bookingingestservice.redis.RedisKeys;

@Repository
public class InventoryRedisRepository {

    private final StringRedisTemplate redisTemplate;
    private final DefaultRedisScript<Long> decrementScript;

    public InventoryRedisRepository(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;

        // Load the Lua script from resources asynchronously at startup
        this.decrementScript = new DefaultRedisScript<>();
        this.decrementScript.setResultType(Long.class);
        this.decrementScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("scripts/decrement_stock.lua")));
    }

    public Long tryAcquireSlot(String itemId, String userId) {

        String inventoryKey = RedisKeys.inventory(itemId);
        String bookedUsersKey = RedisKeys.bookedUsers(itemId);

        return redisTemplate.execute(
                decrementScript,
                List.of(inventoryKey, bookedUsersKey),
                userId
        );

    }

}
