package com.courierpyme.audit.dto;

public record AuditTimelineEvent(
        String eventId,
        Long shipmentId,
        String trackingCode,
        String action,
        String fromStatus,
        String toStatus,
        String actor,
        String occurredAt,
        String recordedAt,
        String sourceTopic,
        Integer sourcePartition,
        Long sourceOffset,
        String recordedBy) {
}
