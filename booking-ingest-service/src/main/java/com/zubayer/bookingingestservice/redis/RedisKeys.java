package com.zubayer.bookingingestservice.redis;

public final class RedisKeys {

    private RedisKeys() {}

    public static final String ACTIVE_ITEMS = "flash-sale:active-items";

    public static String inventory(String itemId) {
        return "item:{" + itemId + "}:inventory";
    }

    public static String bookedUsers(String itemId) {
        return "item:{" + itemId + "}:booked_users";
    }

    public static String outbox(String itemId) {
        return "item:{" + itemId + "}:outbox";
    }

}
