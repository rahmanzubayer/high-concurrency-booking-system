package com.zubayer.bookingingestservice.messaging;

import lombok.extern.slf4j.Slf4j;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import com.zubayer.bookingingestservice.redis.RedisKeys;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;

import java.util.Map;
import java.util.Set;
import java.util.List;
import java.time.Instant;
import java.util.ArrayList;

// Local Imports
import com.zubayer.bookingingestservice.dto.BookingConfirmedEvent;

/**
 * Drains the per-item Redis Stream outbox into Kafka.
 * <p>
 * The Lua script writes the booking event into a Redis Stream in the same atomic
 * execution that decrements inventory, so a slot and its event can never disagree.
 * This relay is the second half of that guarantee: it reads entries from the stream
 * and publishes them to Kafka, acknowledging an entry only AFTER the broker has
 * confirmed the record.
 * <p>
 * Delivery is at-least-once. A crash between publishing and acknowledging leaves the
 * entry in the consumer group's pending list, so it is re-delivered on the next pass
 * and Kafka receives it twice. The worker's unique constraint on transaction_id turns
 * that duplicate into a no-op, which is why the transaction id must be deterministic.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxRelay {

    public static final String CONSUMER_GROUP = "outbox-relay";
    private static final int BATCH_SIZE = 500;

    private final StringRedisTemplate redis;
    private final StreamBridge streamBridge;

    /**
     * Stable per instance, on purpose. A restarted relay must be able to see the
     * entries its previous incarnation took but never acknowledged. A random name
     * would orphan them in the pending list forever. With several relay instances,
     * give each one a distinct value.
     */
    @Value("${outbox.relay.consumer-name:relay-1}")
    private String consumerName;

    @Scheduled(fixedDelay = 100)
    public void drain() {
        Set<String> activeItems = redis.opsForSet().members(RedisKeys.ACTIVE_ITEMS);
        if (activeItems == null || activeItems.isEmpty()) {
            return;
        }
        for (String itemId : activeItems) {
            try {
                drainItem(itemId);
            } catch (Exception e) {
                log.error("Relay pass failed for item {}; will retry next tick", itemId, e);
            }
        }
    }

    private void drainItem(String itemId) {

        String streamKey = RedisKeys.outbox(itemId);

        if (!Boolean.TRUE.equals(redis.hasKey(streamKey))) {
            return;   // sale warmed up but nothing sold yet
        }
        ensureGroup(streamKey);

        // 1. Anything this consumer took previously but never acknowledged.
        //    ReadOffset "0" returns THIS consumer's pending entries, not new ones.
        publish(streamKey, read(streamKey, ReadOffset.from("0")));

        // 2. Then entries nobody has taken yet.
        publish(streamKey, read(streamKey, ReadOffset.lastConsumed()));
    }

    private List<MapRecord<String, Object, Object>> read(String streamKey, ReadOffset offset) {
        return redis.opsForStream().read(
                Consumer.from(CONSUMER_GROUP, consumerName),
                StreamReadOptions.empty().count(BATCH_SIZE),
                StreamOffset.create(streamKey, offset));
    }

    private void publish(String streamKey, List<MapRecord<String, Object, Object>> records) {

        if (records == null || records.isEmpty()) {
            return;
        }

        List<RecordId> sent = new ArrayList<>(records.size());

        try {
            for (MapRecord<String, Object, Object> record : records) {
                // Blocks until the broker acknowledges: see producer.sync in application.yaml
                streamBridge.send("outboundBooking-out-0", toEvent(record));
                sent.add(record.getId());
            }
        } finally {
            // Acknowledge whatever genuinely reached Kafka, even if a later send threw.
            if (!sent.isEmpty()) {
                redis.opsForStream().acknowledge(streamKey, CONSUMER_GROUP,
                        sent.toArray(new RecordId[0]));
                log.debug("Relayed {} entries from {}", sent.size(), streamKey);
            }
        }
    }

    private BookingConfirmedEvent toEvent(MapRecord<String, Object, Object> record) {
        Map<Object, Object> fields = record.getValue();
        return new BookingConfirmedEvent(
                String.valueOf(fields.get("itemId")),
                String.valueOf(fields.get("userId")),
                String.valueOf(fields.get("transactionId")),
                Instant.ofEpochMilli(Long.parseLong(String.valueOf(fields.get("ts")))));
    }

    /**
     * Runs on every pass, deliberately.
     * <p>
     * Creating a group that already exists fails with BUSYGROUP, which is the normal
     * case and is ignored. Caching "this group exists" in the JVM is wrong: warming up
     * a sale deletes the stream, and deleting a stream destroys its consumer groups.
     * A cached flag survives that deletion and leaves the relay reading from a group
     * that no longer exists. One XGROUP CREATE per item per tick is a rounding error.
     */
    private void ensureGroup(String streamKey) {
        try {
            redis.opsForStream().createGroup(streamKey, ReadOffset.from("0"), CONSUMER_GROUP);
            log.info("Created consumer group {} on {}", CONSUMER_GROUP, streamKey);
        } catch (RuntimeException e) {
            // BUSYGROUP — already exists, which is the usual outcome
        }
    }
}
