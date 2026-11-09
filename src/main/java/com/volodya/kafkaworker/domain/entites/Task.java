package com.volodya.kafkaworker.domain.entites;

import com.volodya.kafkaworker.domain.entites.enums.TaskStatus;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.UUID;

public record Task(
    UUID id,
    UUID eventId,
    String name,
    long duration,
    TaskStatus taskStatus,
    int progress,
    @Nullable String result,
    @Nullable UUID executionId,
    Instant createdAt,
    @Nullable Instant startedAt,
    @Nullable Instant finishedAt,
    long version
) {
    public Task(
        UUID eventId,
        String name
    ) {
        this(
            UUID.randomUUID(), eventId, name, 0, TaskStatus.NEW, 0, null,
            null, Instant.now(), null, null, 0
        );
    }
}
