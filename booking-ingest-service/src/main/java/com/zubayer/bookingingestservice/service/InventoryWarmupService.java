package com.zubayer.bookingingestservice.service;

import lombok.extern.slf4j.Slf4j;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import com.zubayer.bookingingestservice.redis.RedisKeys;
import com.zubayer.bookingingestservice.dto.WarmupResult;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.Optional;

// Local Imports
import com.zubayer.bookingingestservice.repository.ProductRepository;


@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryWarmupService {

    private static final Duration KEY_TTL = Duration.ofHours(24);

    private final StringRedisTemplate redisTemplate;
    private final ProductRepository productRepository;

    /**
     * Copies durable stock from PostgreSQL into the Redis counter and clears any
     * previous sale state. Safe to re-run: this is also how you reset a sale.
     */
    public Optional<WarmupResult> warmUp(String itemId) {
        return productRepository.findById(itemId)
                .map(product -> {

                    redisTemplate.delete(RedisKeys.bookedUsers(itemId));

                    redisTemplate.opsForValue().set(
                            RedisKeys.inventory(itemId),
                            String.valueOf(product.getTotalStock()),
                            KEY_TTL
                    );

                    log.info("Warmed up item {} with stock {}", itemId, product.getTotalStock());
                    return new WarmupResult(itemId, product.getName(), product.getTotalStock());
                });
    }

}
