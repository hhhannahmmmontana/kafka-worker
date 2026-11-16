package com.volodya.kafkaworker.persistence.repositories;

import com.volodya.kafkaworker.persistence.entities.TaskEntity;
import com.volodya.kafkaworker.persistence.entities.enums.TaskStatus;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface TaskEntityJpaRepository extends JpaRepository<TaskEntity, Long> {

    Optional<TaskEntity> findFirstByStatusOrderByCreatedAtAsc(TaskStatus status);

    Optional<TaskEntity> findByEventId(UUID eventId);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    Optional<TaskEntity> findForUpdateById(Long id);

    @Modifying(flushAutomatically = true)
    @NativeQuery(value = """
        INSERT INTO tasks (event_id, name, duration)
        VALUES (:eventId, :name, :duration)
        ON CONFLICT (event_id) DO NOTHING
        """)
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
        UPDATE TaskEntity t
        SET t.status = :status,
            t.progress = :progress,
            t.result = :result,
            t.finishedAt = :finishedAt,
            t.leaseUntil = null,
            t.nextRetryAt = :nextRetryAt,
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
        @Param("finishedAt") Instant finishedAt,
        @Param("nextRetryAt") Instant nextRetryAt
    );

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @NativeQuery("""
        WITH failed AS (
            SELECT id
            FROM tasks
            WHERE status = 'FAILED'
                AND (next_retry_at IS NULL OR next_retry_at <= CURRENT_TIMESTAMP)
                AND attempts < max_attempts
            ORDER BY next_retry_at, id
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
        )
        UPDATE tasks t
        SET status = 'NEW',
            execution_id = NULL,
            next_retry_at = NULL,
            started_at = NULL,
            finished_at = NULL,
            progress = 0,
            result = NULL,
            version = t.version + 1
        FROM failed f
        WHERE t.id = f.id;
        """)
    int recoverFailedTasks(@Param("limit") int limit);

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @NativeQuery("""
        WITH expired AS (
            SELECT id
            FROM tasks
            WHERE status = 'IN_PROGRESS'
              AND (lease_until IS NULL OR lease_until < CURRENT_TIMESTAMP)
            ORDER BY lease_until, id
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
        )
        UPDATE tasks t
        SET status = CASE
                WHEN t.attempts < t.max_attempts THEN 'NEW'
                ELSE 'FAILED'
            END,
            execution_id = NULL,
            lease_until = NULL,
            started_at = CASE
                WHEN t.attempts < t.max_attempts THEN NULL
                ELSE t.started_at
            END,
            finished_at = CASE
                WHEN t.attempts >= t.max_attempts THEN CURRENT_TIMESTAMP
                ELSE NULL
            END,
            progress = CASE
                WHEN t.attempts < t.max_attempts THEN 0
                ELSE t.progress
            END,
            result = CASE
                WHEN t.attempts >= t.max_attempts THEN 'Lease time ran out'
                ELSE NULL
            END,
            version = t.version + 1
        FROM expired e
        WHERE t.id = e.id;
        """)
    int recoverStuckTasks(@Param("limit") int limit);
}
