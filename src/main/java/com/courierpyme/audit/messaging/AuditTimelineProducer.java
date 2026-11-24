package com.courierpyme.audit.messaging;

import com.courierpyme.audit.dto.AuditTimelineEvent;
import com.courierpyme.audit.model.AuditEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class AuditTimelineProducer {

    private static final Logger log = LoggerFactory.getLogger(AuditTimelineProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topic;

    public AuditTimelineProducer(@Qualifier("objectKafkaTemplate") KafkaTemplate<String, Object> kafkaTemplate,
                                 @Value("${app.kafka.topic.audit-timeline}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void publicar(AuditEvent evento) {
        AuditTimelineEvent mensaje = new AuditTimelineEvent(
                evento.getEventId(),
                evento.getShipmentId(),
                evento.getTrackingCode(),
                evento.getEventType(),
                evento.getPreviousStatus(),
                evento.getStatus(),
                evento.getActor(),
                evento.getOccurredAt().toString(),
                evento.getReceivedAt().toString(),
                evento.getKafkaTopic(),
                evento.getKafkaPartition(),
                evento.getKafkaOffset(),
                "ms-courierpyme-audit");

        String clave = String.valueOf(evento.getShipmentId());
        try {
            kafkaTemplate.send(topic, clave, mensaje).whenComplete((resultado, error) -> {
                if (error != null) {
                    log.error("No se pudo publicar en {} eventId={} shipmentId={} causa={}",
                            topic, evento.getEventId(), evento.getShipmentId(), error.getMessage());
                } else {
                    log.info("Timeline publicado topic={} partition={} offset={} shipmentId={} eventId={}",
                            topic,
                            resultado.getRecordMetadata().partition(),
                            resultado.getRecordMetadata().offset(),
                            evento.getShipmentId(),
                            evento.getEventId());
                }
            });
        } catch (Exception e) {
            log.error("Error al enviar a {} eventId={} shipmentId={} causa={}",
                    topic, evento.getEventId(), evento.getShipmentId(), e.getMessage());
        }
    }
}
