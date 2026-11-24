package com.courierpyme.audit.messaging;

import com.courierpyme.audit.dto.ShipmentEvent;
import com.courierpyme.audit.exception.InvalidEventException;
import com.courierpyme.audit.service.AuditService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ShipmentEventListener {

    private static final Logger log = LoggerFactory.getLogger(ShipmentEventListener.class);

    private final AuditService auditService;
    private final AuditTimelineProducer timelineProducer;

    public ShipmentEventListener(AuditService auditService, AuditTimelineProducer timelineProducer) {
        this.auditService = auditService;
        this.timelineProducer = timelineProducer;
    }

    @KafkaListener(
            topics = "${app.kafka.topic.shipments-events}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void onShipmentEvent(ConsumerRecord<String, ShipmentEvent> record) {
        ShipmentEvent event = record.value();
        if (event == null) {
            throw new InvalidEventException("Mensaje sin payload en partition=" + record.partition()
                    + " offset=" + record.offset());
        }

        log.info("Evento recibido topic={} partition={} offset={} key={} eventId={} type={} shipmentId={} traceId={}",
                record.topic(), record.partition(), record.offset(), record.key(),
                event.eventId(), event.type(), event.shipmentId(), event.traceId());

        auditService.registrar(event, record.topic(), record.partition(), record.offset())
                .ifPresent(timelineProducer::publicar);
    }
}
