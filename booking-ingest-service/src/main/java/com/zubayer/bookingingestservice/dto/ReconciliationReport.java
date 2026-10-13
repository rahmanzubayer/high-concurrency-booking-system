package com.zubayer.bookingingestservice.dto;

import java.util.List;

public record ReconciliationReport(
        String itemId,
        long slotsGranted,
        long ordersForItem,
        long slotsWithoutOrder,
        long outboxPending,
        long unexplainedGap,
        List<String> sampleMissingUsers
) {}
