package com.volodya.kafkaworker.application.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.annotation.EnableRetry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@EnableRetry
@Configuration
public class AppConfig {

    @Bean("taskExecutorService")
    public ExecutorService executorService(
        @Value("${application.worker.runners.maxAmount:4}") int poolSize
    ) {
        if (poolSize < 1) {
            throw new IllegalArgumentException("Pool size must be at least one");
        }
        return Executors.newFixedThreadPool(poolSize);
    }
}
