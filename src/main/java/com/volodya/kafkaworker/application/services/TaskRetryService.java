package com.volodya.kafkaworker.application.services;

import com.volodya.kafkaworker.application.exceptions.IllegalTaskStateException;
import com.volodya.kafkaworker.application.exceptions.TaskNotFoundException;
import com.volodya.kafkaworker.persistence.entities.enums.TaskStatus;
import com.volodya.kafkaworker.persistence.repositories.TaskEntityJpaRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Slf4j
public class TaskRetryService {

    private final TaskEntityJpaRepository taskRepository;

    @Value("${application.recover-tasks.stuck.limit:100}")
    private int recoverStuckLimit;

    @Value("${application.recover-tasks.failed.limit:100}")
    private int recoverFailedLimit;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void retryFailedTask(long id) {
        var task = taskRepository.findForUpdateById(id)
            .orElseThrow(() -> new TaskNotFoundException(id));
        if (task.getStatus() != TaskStatus.FAILED) {
            throw new IllegalTaskStateException("Can't retry the task with non-failed status");
        }

        task.setStatus(TaskStatus.NEW);
        task.setExecutionId(null);
        task.setProgress(0);
        task.setStartedAt(null);
        task.setFinishedAt(null);
        task.setResult(null);
    }

    @Scheduled(fixedDelayString = "${application.recover-tasks.stuck.delay-ms:5000}")
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void recoverStuckTasks() {
        int affected = taskRepository.recoverStuckTasks(recoverStuckLimit);
        log.info("Recovered {} stuck tasks", affected);
    }

    @Scheduled(fixedDelayString = "${application.recover-tasks.failed.delay-ms:5000}")
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void recoverFailedTasks() {
        int affected = taskRepository.recoverFailedTasks(recoverFailedLimit);
        log.info("Recovered {} failed tasks", affected);
    }
}
