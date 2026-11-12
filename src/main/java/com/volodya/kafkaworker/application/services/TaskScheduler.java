package com.volodya.kafkaworker.application.services;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.bind.Name;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

@Service
@RequiredArgsConstructor
@Slf4j
public class TaskScheduler {

    @Name("taskExecutorService")
    private final ExecutorService executorService;
    private final TaskExecutionService taskExecutionService;
    private final LongAdder workersCounter;
    private volatile boolean stopping = false;

    @Value("${application.worker.runners.maxAmount:4}")
    private int maxWorkersAmount;

    @Scheduled(fixedDelayString = "${application.worker.poll-delay-ms:1000}")
    public void processTask() {
        if (!stopping || executorService.isShutdown()) {
            return;
        }

        var amount = workersCounter.sum();
        while (amount < maxWorkersAmount) {
            try {
                executorService.execute(() -> {
                    workersCounter.increment();
                    runWorker();
                    workersCounter.decrement();
                });
            } catch (RejectedExecutionException ex) {
                log.warn("Executor service can't take any more workers");
                break;
            }
        }
    }

    private void runWorker() {
        try {
            while (!stopping && !Thread.currentThread().isInterrupted()) {
                taskExecutionService.executeNextTask();
            }
        } catch (Exception ex) {
            log.error("Error while executing task", ex);
        }
    }

    @PreDestroy
    public void shutdown() {
        stopping = true;
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.warn("Not all workers completed after termination");
                }
            }
        } catch (InterruptedException ex) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
