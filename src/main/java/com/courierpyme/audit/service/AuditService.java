package com.courierpyme.audit.service;

import com.courierpyme.audit.dto.AuditEventResponse;
import com.courierpyme.audit.dto.AuditPageResponse;
import com.courierpyme.audit.dto.ShipmentEvent;
import com.courierpyme.audit.exception.InvalidEventException;
import com.courierpyme.audit.model.AuditEvent;
import com.courierpyme.audit.repository.AuditEventRepository;
import jakarta.persistence.criteria.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Service
public class AuditService {

    private static final Logger log = LoggerFactory.getLogger(AuditService.class);

    private static final Set<String> TIPOS_VALIDOS = Set.of("SHIPMENT_CREATED", "SHIPMENT_STATUS_CHANGED");

    private final AuditEventRepository repository;

    public AuditService(AuditEventRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public Optional<AuditEvent> registrar(ShipmentEvent event, String topic, int partition, long offset) {
        validar(event);

        if (repository.existsByEventId(event.eventId())) {
            log.warn("Evento duplicado ignorado eventId={} shipmentId={} partition={} offset={}",
                    event.eventId(), event.shipmentId(), partition, offset);
            return Optional.empty();
        }

        AuditEvent entity = new AuditEvent(
                event.eventId(),
                event.type(),
                parsearTimestamp(event.timestamp()),
                Instant.now(),
                event.traceId(),
                event.correlationId(),
                event.shipmentId(),
                event.trackingCode(),
                event.recipient(),
                event.address(),
                event.previousStatus(),
                event.status(),
                event.actor(),
                topic,
                partition,
                offset);

        AuditEvent guardado = repository.save(entity);
        log.info("Evento auditado eventId={} type={} shipmentId={} traceId={} actor={} partition={} offset={}",
                guardado.getEventId(), guardado.getEventType(), guardado.getShipmentId(),
                guardado.getTraceId(), guardado.getActor(), partition, offset);
        return Optional.of(guardado);
    }

    @Transactional(readOnly = true)
    public AuditPageResponse buscar(Long shipmentId, String actor, String type,
                                    Instant from, Instant to, int page, int size) {
        Specification<AuditEvent> spec = (root, query, cb) -> {
            List<Predicate> predicados = new ArrayList<>();
            if (shipmentId != null) {
                predicados.add(cb.equal(root.get("shipmentId"), shipmentId));
            }
            if (actor != null && !actor.isBlank()) {
                predicados.add(cb.equal(root.get("actor"), actor));
            }
            if (type != null && !type.isBlank()) {
                predicados.add(cb.equal(root.get("eventType"), type));
            }
            if (from != null) {
                predicados.add(cb.greaterThanOrEqualTo(root.<Instant>get("occurredAt"), from));
            }
            if (to != null) {
                predicados.add(cb.lessThanOrEqualTo(root.<Instant>get("occurredAt"), to));
            }
            return cb.and(predicados.toArray(new Predicate[0]));
        };

        int paginaSegura = Math.max(page, 0);
        int tamanoSeguro = Math.min(Math.max(size, 1), 100);
        Page<AuditEvent> resultado = repository.findAll(
                spec, PageRequest.of(paginaSegura, tamanoSeguro, Sort.by(Sort.Direction.DESC, "occurredAt")));

        List<AuditEventResponse> contenido = resultado.getContent().stream()
                .map(AuditEventResponse::from)
                .toList();
        return new AuditPageResponse(
                contenido,
                resultado.getNumber(),
                resultado.getSize(),
                resultado.getTotalElements(),
                resultado.getTotalPages());
    }

    @Transactional(readOnly = true)
    public List<AuditEventResponse> timelineDeEnvio(Long shipmentId) {
        return repository.findByShipmentIdOrderByOccurredAtAsc(shipmentId).stream()
                .map(AuditEventResponse::from)
                .toList();
    }

    private void validar(ShipmentEvent event) {
        if (event == null) {
            throw new InvalidEventException("Evento nulo");
        }
        if (esVacio(event.eventId())) {
            throw new InvalidEventException("eventId obligatorio");
        }
        if (esVacio(event.type()) || !TIPOS_VALIDOS.contains(event.type())) {
            throw new InvalidEventException("type invalido: " + event.type());
        }
        if (event.shipmentId() == null) {
            throw new InvalidEventException("shipmentId obligatorio para eventId=" + event.eventId());
        }
    }

    private boolean esVacio(String valor) {
        return valor == null || valor.isBlank();
    }

    private Instant parsearTimestamp(String valor) {
        if (esVacio(valor)) {
            return Instant.now();
        }
        try {
            return Instant.parse(valor);
        } catch (Exception ignorada) {
            try {
                return OffsetDateTime.parse(valor).toInstant();
            } catch (Exception ignorada2) {
                try {
                    return LocalDateTime.parse(valor).toInstant(ZoneOffset.UTC);
                } catch (Exception ignorada3) {
                    log.warn("Timestamp no parseable '{}', se usa la hora de recepcion", valor);
                    return Instant.now();
                }
            }
        }
    }
}
