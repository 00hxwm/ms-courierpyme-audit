package com.courierpyme.audit.repository;

import com.courierpyme.audit.model.AuditEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

import java.util.List;

public interface AuditEventRepository extends JpaRepository<AuditEvent, Long>, JpaSpecificationExecutor<AuditEvent> {

    boolean existsByEventId(String eventId);

    List<AuditEvent> findByShipmentIdOrderByOccurredAtAsc(Long shipmentId);
}
