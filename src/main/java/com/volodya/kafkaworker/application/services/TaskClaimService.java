package com.volodya.kafkaworker.application.services;

import com.volodya.kafkaworker.application.entites.TaskRun;
import com.volodya.kafkaworker.persistence.entities.enums.TaskStatus;
import com.volodya.kafkaworker.persistence.repositories.TaskEntityJpaRepository;
import jakarta.persistence.OptimisticLockException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class TaskClaimService {

    private final TaskEntityJpaRepository taskRepository;

    @Retryable(
        retryFor = {
            OptimisticLockingFailureException.class,
            OptimisticLockException.class
        },
        maxAttempts = 3,
        backoff = @Backoff(delay = 100, multiplier = 2, maxDelay = 1000)
    )
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Optional<TaskRun> claimNextTask() {
        return taskRepository.findByStatusOrderByCreatedAtAsc(TaskStatus.NEW)
            .map(taskEntity -> {
                taskEntity.setStatus(TaskStatus.IN_PROGRESS);
                taskEntity.setExecutionId(UUID.randomUUID());
                taskEntity.setStartedAt(Instant.now());
                taskEntity.setProgress(0);
                taskRepository.saveAndFlush(taskEntity);

                return new TaskRun(
                    taskEntity.getId(),
                    taskEntity.getExecutionId(),
                    taskEntity.getName(),
                    taskEntity.getDuration()
                );
            });
    }

    @Recover
    public Optional<TaskRun> recover(OptimisticLockingFailureException ex) {
        return recoverInternal(ex);
    }

    @Recover
    public Optional<TaskRun> recover(OptimisticLockException ex) {
        return recoverInternal(ex);
    }

    private Optional<TaskRun> recoverInternal(Exception ex) {
        log.warn("Optimistic lock failure", ex);
        return Optional.empty();
    }
}
