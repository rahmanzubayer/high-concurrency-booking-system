package com.zubayer.bookingingestservice.redis;

public final class RedisKeys {

    private RedisKeys() {}

    public static String inventory(String itemId) {
        return "item:{" + itemId + "}:inventory";
    }

    public static String bookedUsers(String itemId) {
        return "item:{" + itemId + "}:booked_users";
    }

}
