package com.volodya.kafkaworker.infrastructure.persistence.repositories;

import com.volodya.kafkaworker.infrastructure.persistence.entities.TaskEntity;
import com.volodya.kafkaworker.infrastructure.persistence.entities.enums.TaskStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface TaskRepository extends JpaRepository<TaskEntity, Long> {

    Optional<TaskEntity> findFirstByStatusOrderByCreationDateAsc(TaskStatus status);

    Optional<TaskEntity> findByEventId(UUID eventId);

    @Modifying(flushAutomatically = true)
    @Query(value = """
        INSERT INTO tasks (event_id, name, duration)
        VALUES (:eventId, :name, :duration)
        ON CONFLICT (event_id) DO NOTHING
        """, nativeQuery = true)
    int insertIfAbsent(
        @Param("eventId") UUID eventId,
        @Param("name") String name,
        @Param("duration") Long duration
    );
}
