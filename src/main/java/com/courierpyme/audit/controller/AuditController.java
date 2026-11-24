package com.courierpyme.audit.controller;

import com.courierpyme.audit.dto.AuditEventResponse;
import com.courierpyme.audit.dto.AuditPageResponse;
import com.courierpyme.audit.service.AuditService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;

@RestController
@RequestMapping("/api/audit")
public class AuditController {

    private final AuditService auditService;

    public AuditController(AuditService auditService) {
        this.auditService = auditService;
    }

    @GetMapping
    public AuditPageResponse listar(
            @RequestParam(required = false) Long shipmentId,
            @RequestParam(required = false) String actor,
            @RequestParam(required = false) String type,
            @RequestParam(required = false) Instant from,
            @RequestParam(required = false) Instant to,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        return auditService.buscar(shipmentId, actor, type, from, to, page, size);
    }

    @GetMapping("/shipments/{shipmentId}")
    public List<AuditEventResponse> timeline(@PathVariable Long shipmentId) {
        return auditService.timelineDeEnvio(shipmentId);
    }
}
