package com.zubayer.bookingingestservice.dto;

public enum AcquireStatus {

    ACQUIRED,
    SOLD_OUT,
    ALREADY_BOOKED,
    NOT_AVAILABLE,
    UNKNOWN;

    public static AcquireStatus from(Long code) {
        if (code == null) {
            return UNKNOWN; // nil reply from Redis — never unbox blindly
        }

        return switch (code.intValue()) {
            case 1 -> ACQUIRED;
            case 0 -> SOLD_OUT;
            case -1 -> ALREADY_BOOKED;
            case -2 -> NOT_AVAILABLE;
            default -> UNKNOWN;
        };
    }

}
