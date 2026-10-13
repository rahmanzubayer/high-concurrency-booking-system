package com.zubayer.bookingingestservice.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import com.zubayer.bookingingestservice.dto.WarmupResult;

// Local Imports
import com.zubayer.bookingingestservice.dto.ReconciliationReport;
import com.zubayer.bookingingestservice.service.ReconciliationService;
import com.zubayer.bookingingestservice.service.InventoryWarmupService;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/admin/sales")
public class SaleAdminController {

    private final InventoryWarmupService warmupService;
    private final ReconciliationService reconciliationService;

    @PostMapping("/{itemId}/warmup")
    public ResponseEntity<WarmupResult> warmUp(@PathVariable String itemId) {
        return warmupService.warmUp(itemId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.<WarmupResult>notFound().build());
    }

    @GetMapping("/{itemId}/reconcile")
    public ReconciliationReport reconcile(@PathVariable String itemId) {
        return reconciliationService.reconcile(itemId);
    }

}
