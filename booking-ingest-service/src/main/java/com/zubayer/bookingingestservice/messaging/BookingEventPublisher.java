package com.zubayer.bookingingestservice.messaging;

import lombok.extern.slf4j.Slf4j;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.cloud.stream.function.StreamBridge;

import java.time.Instant;

// Local Imports
import com.zubayer.bookingingestservice.dto.BookingConfirmedEvent;

@Slf4j
@Service
@RequiredArgsConstructor
public class BookingEventPublisher {

    private final StreamBridge streamBridge;

    public void publishBookingConfirmation(String itemId, String userId, String transactionId) {

        BookingConfirmedEvent event = new BookingConfirmedEvent(
                itemId,
                userId,
                transactionId,
                Instant.now()
        );

        log.info("Publishing BookingConfirmedEvent for itemId: {} and userId: {}", itemId, userId);

        // Pushes the payload directly to the binding configured in our application.yml
        streamBridge.send("outboundBooking-out-0", event);

    }

}
