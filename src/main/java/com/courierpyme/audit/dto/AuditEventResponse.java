package com.courierpyme.audit.dto;

import com.courierpyme.audit.model.AuditEvent;

import java.time.Instant;

public record AuditEventResponse(
        Long id,
        String eventId,
        String eventType,
        Instant occurredAt,
        Instant receivedAt,
        String traceId,
        String correlationId,
        Long shipmentId,
        String trackingCode,
        String recipient,
        String address,
        String previousStatus,
        String status,
        String actor,
        String kafkaTopic,
        Integer kafkaPartition,
        Long kafkaOffset) {

    public static AuditEventResponse from(AuditEvent e) {
        return new AuditEventResponse(
                e.getId(),
                e.getEventId(),
                e.getEventType(),
                e.getOccurredAt(),
                e.getReceivedAt(),
                e.getTraceId(),
                e.getCorrelationId(),
                e.getShipmentId(),
                e.getTrackingCode(),
                e.getRecipient(),
                e.getAddress(),
                e.getPreviousStatus(),
                e.getStatus(),
                e.getActor(),
                e.getKafkaTopic(),
                e.getKafkaPartition(),
                e.getKafkaOffset());
    }
}
