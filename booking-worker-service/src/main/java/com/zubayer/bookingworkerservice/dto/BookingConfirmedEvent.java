package com.zubayer.bookingworkerservice.dto;

import java.time.Instant;

public record BookingConfirmedEvent(
        String itemId,
        String userId,
        String transactionId,
        Instant timestamp
) {}
