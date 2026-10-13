package com.zubayer.bookingingestservice.service;

import com.zubayer.bookingingestservice.dto.ReconciliationReport;
import com.zubayer.bookingingestservice.messaging.OutboxRelay;
import com.zubayer.bookingingestservice.redis.RedisKeys;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.PendingMessagesSummary;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Compares what Redis believes it sold against what PostgreSQL durably recorded.
 * <p>
 * Every user in booked_users was granted a slot. Every such user should eventually have an
 * order row. "Eventually" is the difficult word: the event may still be sitting in the Redis
 * outbox, in flight to Kafka, or waiting for the worker to consume it. A gap at any single
 * moment is normal.
 * <p>
 * So this reports:
 * <p>
 * unexplainedGap = slotsGranted - ordersPersisted - outboxPending
 * <p>
 * The outbox term accounts for entries the relay has not yet delivered. What it does NOT
 * account for is events already published to Kafka but not yet consumed by the worker, so
 * the gap is still expected to be briefly positive after a sale and to fall to zero on its
 * own.
 * <p>
 * That is the honest limit of this check, and it reflects something true about every
 * eventually consistent system: at a single instant you cannot distinguish "late" from
 * "lost". Only time distinguishes them. The judgement therefore does not belong in this
 * code — it belongs in an alert rule that fires when the gauge stays non-zero across
 * several consecutive scrapes.
 * <p>
 * What this catches, and nothing else does: a Lua script that failed partway through, after
 * decrementing inventory and adding the user but before (or while) writing the outbox entry.
 * Redis does not roll back, so the slot is consumed and no event exists. Nothing else in the
 * system can detect that.
 */
@Slf4j
@Service
public class ReconciliationService {

    private static final int SAMPLE_LIMIT = 20;
    private static final int SCAN_BATCH = 500;

    private final StringRedisTemplate redis;
    private final JdbcTemplate jdbcTemplate;
    private final MeterRegistry registry;

    private final Map<String, AtomicLong> gapGauges = new ConcurrentHashMap<>();

    public ReconciliationService(StringRedisTemplate redis,
                                 JdbcTemplate jdbcTemplate,
                                 MeterRegistry registry) {
        this.redis = redis;
        this.jdbcTemplate = jdbcTemplate;
        this.registry = registry;
    }

    @Scheduled(fixedDelay = 60_000, initialDelay = 30_000)
    public void reconcileActiveItems() {
        Set<String> activeItems = redis.opsForSet().members(RedisKeys.ACTIVE_ITEMS);
        if (activeItems == null || activeItems.isEmpty()) {
            return;
        }
        for (String itemId : activeItems) {
            try {
                ReconciliationReport report = reconcile(itemId);
                if (report.unexplainedGap() > 0) {
                    log.warn("Reconciliation gap for item {}: {} slots granted, {} of them " +
                                    "without an order, {} still in outbox, {} unexplained. Sample: {}",
                            itemId, report.slotsGranted(), report.slotsWithoutOrder(),
                            report.outboxPending(), report.unexplainedGap(),
                            report.sampleMissingUsers());
                }
            } catch (Exception e) {
                log.error("Reconciliation failed for item {}", itemId, e);
            }
        }
    }

    public ReconciliationReport reconcile(String itemId) {

        Set<String> slotHolders = scanBookedUsers(itemId);

        Set<String> withOrders = new HashSet<>(jdbcTemplate.queryForList(
                "SELECT user_id FROM orders WHERE item_id = ?", String.class, itemId));

        // A SET DIFFERENCE, not a subtraction of totals.
        //
        // The orders table accumulates across every sale of this item, while booked_users is
        // cleared at each warm-up. Comparing the two counts is meaningless the moment an item
        // has been sold more than once: a healthy system shows far more orders than current
        // slot holders, and the subtraction goes negative and hides real divergence.
        //
        // The only correct question is "which slot holders have no order?", which is answered
        // per user, not in aggregate.
        List<String> missing = new ArrayList<>();
        long slotsWithoutOrder = 0;
        for (String userId : slotHolders) {
            if (!withOrders.contains(userId)) {
                slotsWithoutOrder++;
                if (missing.size() < SAMPLE_LIMIT) {
                    missing.add(userId);
                }
            }
        }

        long pending = outboxPending(itemId);
        long unexplained = Math.max(0, slotsWithoutOrder - pending);

        publishGauge(itemId, unexplained);

        return new ReconciliationReport(itemId, slotHolders.size(), withOrders.size(),
                slotsWithoutOrder, pending, unexplained, missing);
    }

    /**
     * SSCAN, not SMEMBERS. A sold-out flash sale can leave hundreds of thousands of entries
     * in this set, and SMEMBERS would return all of them in one blocking call on Redis's
     * single thread — stalling every booking in flight. SSCAN walks the set in batches and
     * lets other commands interleave.
     */
    private Set<String> scanBookedUsers(String itemId) {
        Set<String> users = new HashSet<>();
        ScanOptions options = ScanOptions.scanOptions().count(SCAN_BATCH).build();
        try (Cursor<String> cursor = redis.opsForSet().scan(RedisKeys.bookedUsers(itemId), options)) {
            while (cursor.hasNext()) {
                users.add(cursor.next());
            }
        }
        return users;
    }

    /**
     * Entries the relay has read but not yet acknowledged, plus entries never read.
     */
    private long outboxPending(String itemId) {
        String streamKey = RedisKeys.outbox(itemId);
        if (!Boolean.TRUE.equals(redis.hasKey(streamKey))) {
            return 0;
        }
        try {
            PendingMessagesSummary summary =
                    redis.opsForStream().pending(streamKey, OutboxRelay.CONSUMER_GROUP);
            return summary == null ? 0 : summary.getTotalPendingMessages();
        } catch (RuntimeException e) {
            return 0;   // NOGROUP: the relay has not started on this stream yet
        }
    }

    private void publishGauge(String itemId, long gap) {
        gapGauges.computeIfAbsent(itemId, id -> {
            AtomicLong holder = new AtomicLong();
            Gauge.builder("booking.reconciliation.unexplained.gap", holder, AtomicLong::get)
                    .description("Slots granted with no order and no pending outbox entry")
                    .tag("item", id)
                    .register(registry);
            return holder;
        }).set(gap);
    }
}
