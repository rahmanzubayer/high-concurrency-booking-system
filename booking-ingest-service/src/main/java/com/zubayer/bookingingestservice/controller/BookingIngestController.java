package com.zubayer.bookingingestservice.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

// Local Imports
import com.zubayer.bookingingestservice.dto.AcquireResult;
import com.zubayer.bookingingestservice.messaging.BookingEventPublisher;
import com.zubayer.bookingingestservice.repository.InventoryRedisRepository;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/bookings")
public class BookingIngestController {

    private final InventoryRedisRepository inventoryRedisRepository;
    private final BookingEventPublisher bookingEventPublisher;

    @PostMapping(path = "/items/{itemId}")
    public ResponseEntity<Map<String, String>> secureBookingSlot(@PathVariable String itemId, @RequestHeader("X-User-Id") String userId) {

        AcquireResult result = inventoryRedisRepository.tryAcquireSlot(itemId, userId);

        return switch (result.status()) {

            case ACQUIRED -> {
                bookingEventPublisher.publishBookingConfirmation(itemId, userId, result.transactionId());
                yield ResponseEntity.status(HttpStatus.ACCEPTED)
                        .body(Map.of("status", "SUCCESS",
                                "transactionId", result.transactionId(),
                                "message", "Booking confirmed. Processing order..."));
            }

            case ALREADY_BOOKED -> ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of("status", "DUPLICATE",
                            "message", "You have already booked this item."));

            case SOLD_OUT -> ResponseEntity.status(HttpStatus.GONE)
                    .body(Map.of("status", "SOLD_OUT",
                            "message", "Item is completely sold out."));

            case NOT_AVAILABLE -> ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("status", "NOT_AVAILABLE",
                            "message", "This sale is not open."));

            case UNKNOWN -> ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("status", "BUSY",
                            "message", "System busy, please retry."));
        };

    }

}
