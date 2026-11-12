package com.volodya.kafkaworker.persistence.repositories;

import com.volodya.kafkaworker.persistence.entities.TaskEntity;
import com.volodya.kafkaworker.persistence.entities.enums.TaskStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface TaskEntityJpaRepository extends JpaRepository<TaskEntity, Long> {

    Optional<TaskEntity> findByStatusOrderByCreatedAtAsc(TaskStatus status);

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

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query("""
        UPDATE TaskEntity t
        SET t.progress = :progress,
            t.version = t.version + 1
        WHERE t.id = :id
            AND t.executionId = :executionId
            AND t.status = TaskStatus.IN_PROGRESS
            AND t.progress <= :progress
        """)
    int updateProgress(
        @Param("id") long id,
        @Param("executionId") UUID executionId,
        @Param("progress") int progress
    );

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query("""
        UPDATE Task t
        SET t.status = :status,
            t.progress = :progress,
            t.result = :result,
            t.finishedAt = :finishedAt,
            t.version = t.version + 1
        WHERE t.id = :id
          AND t.executionId = :executionId
          AND t.status = TaskStatus.IN_PROGRESS
        """)
    int finishExecution(
        @Param("id") long id,
        @Param("executionId") UUID executionId,
        @Param("status") TaskStatus status,
        @Param("progress") int progress,
        @Param("result") String result,
        @Param("finishedAt") Instant finishedAt
    );
}
