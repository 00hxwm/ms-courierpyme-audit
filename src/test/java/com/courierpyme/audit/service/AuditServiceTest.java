package com.courierpyme.audit.service;

import com.courierpyme.audit.dto.ShipmentEvent;
import com.courierpyme.audit.exception.InvalidEventException;
import com.courierpyme.audit.model.AuditEvent;
import com.courierpyme.audit.repository.AuditEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AuditServiceTest {

    @Mock
    private AuditEventRepository repository;

    private AuditService service;

    @BeforeEach
    void setUp() {
        service = new AuditService(repository);
    }

    private ShipmentEvent evento(String eventId, String type, String timestamp, Long shipmentId) {
        return new ShipmentEvent(eventId, type, timestamp, "trace-1", "corr-1", shipmentId,
                "TRK-1", "Juan Perez", "Av. Siempre Viva 123", "CREADO", "ACEPTADO", "admin@courierpyme.cl");
    }

    @Test
    void registraEventoValidoConSusDatos() {
        when(repository.existsByEventId("e-1")).thenReturn(false);
        when(repository.save(any(AuditEvent.class))).thenAnswer(invocation -> invocation.getArgument(0));

        Optional<AuditEvent> resultado = service.registrar(
                evento("e-1", "SHIPMENT_STATUS_CHANGED", "2026-11-23T22:00:00Z", 5L), "shipments.events", 1, 10L);

        assertThat(resultado).isPresent();
        ArgumentCaptor<AuditEvent> captor = ArgumentCaptor.forClass(AuditEvent.class);
        verify(repository).save(captor.capture());
        AuditEvent guardado = captor.getValue();
        assertThat(guardado.getEventId()).isEqualTo("e-1");
        assertThat(guardado.getShipmentId()).isEqualTo(5L);
        assertThat(guardado.getActor()).isEqualTo("admin@courierpyme.cl");
        assertThat(guardado.getPreviousStatus()).isEqualTo("CREADO");
        assertThat(guardado.getStatus()).isEqualTo("ACEPTADO");
        assertThat(guardado.getKafkaPartition()).isEqualTo(1);
        assertThat(guardado.getKafkaOffset()).isEqualTo(10L);
        assertThat(guardado.getOccurredAt()).isEqualTo(Instant.parse("2026-11-23T22:00:00Z"));
    }

    @Test
    void ignoraEventoDuplicado() {
        when(repository.existsByEventId("e-2")).thenReturn(true);

        Optional<AuditEvent> resultado = service.registrar(
                evento("e-2", "SHIPMENT_CREATED", "2026-11-23T22:00:00Z", 7L), "shipments.events", 0, 3L);

        assertThat(resultado).isEmpty();
        verify(repository, never()).save(any(AuditEvent.class));
    }

    @Test
    void rechazaEventoSinEventId() {
        assertThatThrownBy(() -> service.registrar(
                evento(null, "SHIPMENT_CREATED", "2026-11-23T22:00:00Z", 7L), "shipments.events", 0, 1L))
                .isInstanceOf(InvalidEventException.class);
        verify(repository, never()).save(any(AuditEvent.class));
    }

    @Test
    void rechazaEventoConTipoDesconocido() {
        assertThatThrownBy(() -> service.registrar(
                evento("e-3", "OTRO_TIPO", "2026-11-23T22:00:00Z", 7L), "shipments.events", 0, 1L))
                .isInstanceOf(InvalidEventException.class);
    }

    @Test
    void rechazaEventoSinShipmentId() {
        assertThatThrownBy(() -> service.registrar(
                evento("e-4", "SHIPMENT_CREATED", "2026-11-23T22:00:00Z", null), "shipments.events", 0, 1L))
                .isInstanceOf(InvalidEventException.class);
    }

    @Test
    void aceptaTimestampSinZonaHoraria() {
        when(repository.existsByEventId("e-5")).thenReturn(false);
        when(repository.save(any(AuditEvent.class))).thenAnswer(invocation -> invocation.getArgument(0));

        Optional<AuditEvent> resultado = service.registrar(
                evento("e-5", "SHIPMENT_CREATED", "2026-11-23T22:00:00", 9L), "shipments.events", 2, 4L);

        assertThat(resultado).isPresent();
        assertThat(resultado.get().getOccurredAt()).isEqualTo(Instant.parse("2026-11-23T22:00:00Z"));
    }
}
