package com.zubayer.bookingingestservice.metrics;

import io.micrometer.core.instrument.Counter;
import org.springframework.stereotype.Component;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.Map;
import java.util.EnumMap;

// local imports
import com.zubayer.bookingingestservice.dto.AcquireStatus;

@Component
public class BookingMetrics {

    private final Map<AcquireStatus, Counter> attempts = new EnumMap<>(AcquireStatus.class);
    private final Counter outboxPublished;
    private final Counter relayErrors;

    public BookingMetrics(MeterRegistry registry) {

        // Register every outcome up front, including ones that may never occur.
        for (AcquireStatus status : AcquireStatus.values()) {
            attempts.put(
                    status,
                    Counter.builder("booking.attempts")
                            .description("Booking attempts by outcome")
                            .tag("result", status.name().toLowerCase())
                            .register(registry)
            );
        }

        this.outboxPublished = Counter.builder("booking.outbox.published")
                .description("Outbox entries successfully relayed to Kafka")
                .register(registry);

        this.relayErrors = Counter.builder("booking.outbox.relay.errors")
                .description("Relay passes that failed")
                .register(registry);

    }

    public void recordAttempt(AcquireStatus status) {
        attempts.get(status).increment();
    }

    public void recordPublished(int count) {
        outboxPublished.increment(count);
    }

    public void recordRelayErrors() {
        relayErrors.increment();
    }

}
