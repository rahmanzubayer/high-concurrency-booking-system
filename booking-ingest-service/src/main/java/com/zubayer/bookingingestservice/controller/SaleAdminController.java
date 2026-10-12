package com.zubayer.bookingingestservice.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import com.zubayer.bookingingestservice.dto.WarmupResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;

// Local Imports
import com.zubayer.bookingingestservice.service.InventoryWarmupService;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/admin/sales")
public class SaleAdminController {

    private final InventoryWarmupService warmupService;

    @PostMapping("/{itemId}/warmup")
    public ResponseEntity<WarmupResult> warmUp(@PathVariable String itemId) {
        return warmupService.warmUp(itemId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.<WarmupResult>notFound().build());
    }

}
