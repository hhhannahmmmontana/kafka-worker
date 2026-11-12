package com.volodya.kafkaworker.application.services;

import com.volodya.kafkaworker.application.entites.TaskRun;
import com.volodya.kafkaworker.persistence.entities.enums.TaskStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Slf4j
public class TaskExecutionService {

    private final TaskClaimService taskClaimService;
    private final TaskUpdateService taskUpdateService;

    @Value("${application.task.update.delay}")
    private long updateDelay;

    @Transactional
    public void executeNextTask() {
        taskClaimService.claimNextTask().ifPresent(task -> {
            var elapsed = 0L;
            while (elapsed < task.duration()) {
                var executionTime = Math.min(task.duration() - elapsed, updateDelay);
                try {
                    Thread.sleep(executionTime);
                } catch (InterruptedException ex) {
                    log.info("Execution interrupted", ex);
                    failExecution(task, elapsedToProgress(task, elapsed), ex.getMessage());
                    return;
                }

                elapsed += executionTime;
                try {
                    if (!taskUpdateService.updateProgress(task, elapsedToProgress(task, elapsed))) {
                        log.warn(
                            "Execution with id {} for task {} is not active anymore",
                            task.executionId(),
                            task.id()
                        );
                        return;
                    }
                } catch (DataAccessException | TransactionException ex) {
                    log.error("Can't save intermediate state of a task", ex);
                } catch (Exception ex) {
                    log.error("Error occurred while completing the task {}", task.id(), ex);
                    failExecution(
                        task,
                        elapsed,
                        "Unknown error occurred while completing the task"
                    );
                    return;
                }
            }

            taskUpdateService.finishExecution(
                task,
                TaskStatus.COMPLETED,
                100,
                "Task completed successfully"
            );
        });
    }

    private void failExecution(TaskRun task, long elapsed, String result) {
        taskUpdateService.finishExecution(task, TaskStatus.FAILED, elapsedToProgress(task, elapsed), result);
    }

    private int elapsedToProgress(TaskRun task, long elapsed) {
        return (int) Math.max(elapsed * 100L / task.duration(), 100L);
    }
}
