package com.volodya.kafkaworker.application.services;

import com.volodya.kafkaworker.application.entites.TaskRun;
import com.volodya.kafkaworker.persistence.entities.enums.TaskStatus;
import com.volodya.kafkaworker.persistence.repositories.TaskEntityJpaRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Service
@RequiredArgsConstructor
@Transactional(propagation = Propagation.REQUIRES_NEW)
@Slf4j
public class TaskUpdateService {

    private final TaskEntityJpaRepository taskRepository;

    @Value("${application.retry.lag-ms:10000}")
    private long retryLag;

    public boolean updateProgress(TaskRun task, int progress) {
        log.trace("Updating task №{} (eventId: {})", task.id(), task.executionId());
        if (progress < 0 || progress > 99) {
            throw new IllegalArgumentException("Intermediate progress must be in [0; 99]");
        }
        return taskRepository.updateProgress(task.id(), task.executionId(), progress) == 1;
    }

    public boolean finishExecution(
        TaskRun task,
        TaskStatus status,
        int progress,
        String result
    ) {
        if (!List.of(TaskStatus.COMPLETED, TaskStatus.FAILED).contains(status)) {
            throw new IllegalArgumentException("Finished task status expected");
        }
        if (progress < 0 || progress > 100) {
            throw new IllegalArgumentException("Progress must be in [0; 100]");
        }
        Instant nextRetryAt = null;
        if (status == TaskStatus.FAILED) {
            nextRetryAt = Instant.now().plusMillis(retryLag);
        }
        var response = taskRepository.finishExecution(
            task.id(),
            task.executionId(),
            status,
            progress,
            result,
            Instant.now(),
            nextRetryAt
        ) == 1;
        if (response) {
            log.info("Finished task №{} (eventId: {}), status: {}", task.id(), task.executionId(), status);
        }
        return response;
    }
}
