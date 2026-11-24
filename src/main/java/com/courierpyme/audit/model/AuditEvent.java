package com.courierpyme.audit.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

import java.time.Instant;

@Entity
@Table(
        name = "audit_events",
        indexes = {
                @Index(name = "idx_audit_shipment", columnList = "shipment_id"),
                @Index(name = "idx_audit_occurred", columnList = "occurred_at")
        },
        uniqueConstraints = @UniqueConstraint(name = "uk_audit_event_id", columnNames = "event_id"))
public class AuditEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, length = 100)
    private String eventId;

    @Column(name = "event_type", nullable = false, length = 60)
    private String eventType;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    @Column(name = "trace_id", length = 100)
    private String traceId;

    @Column(name = "correlation_id", length = 100)
    private String correlationId;

    @Column(name = "shipment_id", nullable = false)
    private Long shipmentId;

    @Column(name = "tracking_code", length = 100)
    private String trackingCode;

    @Column(name = "recipient", length = 255)
    private String recipient;

    @Column(name = "address", length = 1000)
    private String address;

    @Column(name = "previous_status", length = 40)
    private String previousStatus;

    @Column(name = "status", length = 40)
    private String status;

    @Column(name = "actor", length = 255)
    private String actor;

    @Column(name = "kafka_topic", length = 255)
    private String kafkaTopic;

    @Column(name = "kafka_partition")
    private Integer kafkaPartition;

    @Column(name = "kafka_offset")
    private Long kafkaOffset;

    protected AuditEvent() {
    }

    public AuditEvent(String eventId, String eventType, Instant occurredAt, Instant receivedAt,
                      String traceId, String correlationId, Long shipmentId, String trackingCode,
                      String recipient, String address, String previousStatus, String status,
                      String actor, String kafkaTopic, Integer kafkaPartition, Long kafkaOffset) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.occurredAt = occurredAt;
        this.receivedAt = receivedAt;
        this.traceId = traceId;
        this.correlationId = correlationId;
        this.shipmentId = shipmentId;
        this.trackingCode = trackingCode;
        this.recipient = recipient;
        this.address = address;
        this.previousStatus = previousStatus;
        this.status = status;
        this.actor = actor;
        this.kafkaTopic = kafkaTopic;
        this.kafkaPartition = kafkaPartition;
        this.kafkaOffset = kafkaOffset;
    }

    public Long getId() {
        return id;
    }

    public String getEventId() {
        return eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public Instant getReceivedAt() {
        return receivedAt;
    }

    public String getTraceId() {
        return traceId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public Long getShipmentId() {
        return shipmentId;
    }

    public String getTrackingCode() {
        return trackingCode;
    }

    public String getRecipient() {
        return recipient;
    }

    public String getAddress() {
        return address;
    }

    public String getPreviousStatus() {
        return previousStatus;
    }

    public String getStatus() {
        return status;
    }

    public String getActor() {
        return actor;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public Integer getKafkaPartition() {
        return kafkaPartition;
    }

    public Long getKafkaOffset() {
        return kafkaOffset;
    }
}
