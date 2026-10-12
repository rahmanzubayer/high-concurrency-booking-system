package com.zubayer.gatewayservice.filter;

import reactor.core.publisher.Mono;
import lombok.RequiredArgsConstructor;
import org.springframework.core.Ordered;
import org.springframework.http.MediaType;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.nio.charset.StandardCharsets;

/**
 * Edge admission control.
 * <p>
 * Once an item is sold out the answer is the same for every subsequent request, so
 * there is no reason to spend a rate-limiter token, a network hop and a Lua execution
 * discovering it 9,900 more times.
 * <p>
 * This filter is an OPTIMISATION, never the authority. The Lua script remains the only
 * thing that decides who gets a slot. That is what makes a short-lived local cache safe.
 */
@Component
@RequiredArgsConstructor
public class SoldOutFilter implements GlobalFilter, Ordered {

    private static final Pattern ITEM_PATH =
            Pattern.compile("^/api/v1/bookings/items/([^/]+)$");

    /**
     * Mirrors RedisKeys in booking-ingest-service. Duplicated because these services
     * share no common module; a shared contracts artifact is the proper fix.
     */
    private static String soldOutKey(String itemId) {
        return "item:{" + itemId + "}:soldout";
    }

    private final ReactiveStringRedisTemplate redis;

    private final Cache<String, Boolean> soldOutCache = Caffeine.newBuilder()
            .expireAfterWrite(Duration.ofMillis(250))
            .maximumSize(10_000)
            .build();

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {

        Matcher matcher = ITEM_PATH.matcher(exchange.getRequest().getPath().value());
        if (!matcher.matches()) {
            return chain.filter(exchange);
        }
        String itemId = matcher.group(1);

        Boolean cached = soldOutCache.getIfPresent(itemId);
        if (cached != null) {
            return cached ? reject(exchange) : chain.filter(exchange);
        }

        return redis.hasKey(soldOutKey(itemId))
                .defaultIfEmpty(false)
                .onErrorReturn(false)          // cache-layer trouble must never stop a sale
                .flatMap(soldOut -> {
                    soldOutCache.put(itemId, soldOut);
                    return soldOut ? reject(exchange) : chain.filter(exchange);
                });
    }

    private Mono<Void> reject(ServerWebExchange exchange) {
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(HttpStatus.GONE);
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        response.getHeaders().add("X-Admission", "rejected-at-edge");

        byte[] body = "{\"status\":\"SOLD_OUT\",\"message\":\"Item is completely sold out.\"}".getBytes(StandardCharsets.UTF_8);
        return response.writeWith(Mono.just(response.bufferFactory().wrap(body)));
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 100;   // before the rate limiter
    }
}
