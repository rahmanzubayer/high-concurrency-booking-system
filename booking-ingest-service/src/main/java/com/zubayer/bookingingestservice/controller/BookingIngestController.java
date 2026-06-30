package com.zubayer.bookingingestservice.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

// Local Imports
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

        Long result = inventoryRedisRepository.tryAcquireSlot(itemId, userId);

        if (result == 1) {
            // Asynchronously dispatch to Kafka buffer
            bookingEventPublisher.publishBookingConfirmation(itemId, userId);

            return ResponseEntity
                    .status(HttpStatus.ACCEPTED)
                    .body(Map.of("status", "SUCCESS", "message", "Booking confirmed. Processing order..."));
        } else if (result == -1) {
            return ResponseEntity
                    .status(HttpStatus.CONFLICT)
                    .body(Map.of("status", "DUPLICATE", "message", "You have already booked this item."));
        } else {
            return ResponseEntity
                    .status(HttpStatus.GONE)
                    .body(Map.of("status", "SOLD_OUT", "message", "Item is completely sold out."));
        }

    }

}
