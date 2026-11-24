package com.courierpyme.audit.dto;

public record ShipmentEvent(
        String eventId,
        String type,
        String timestamp,
        String traceId,
        String correlationId,
        Long shipmentId,
        String trackingCode,
        String recipient,
        String address,
        String previousStatus,
        String status,
        String actor) {
}
