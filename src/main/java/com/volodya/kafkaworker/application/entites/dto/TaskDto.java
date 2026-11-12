package com.volodya.kafkaworker.application.entites.dto;

import com.volodya.kafkaworker.persistence.entities.enums.TaskStatus;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.UUID;

public record TaskDto(
    long id,
    UUID eventId,
    String name,
    @Schema(description = "Duration in ms") long duration,
    TaskStatus status,
    int progress,
    String result,
    Instant createdAt,
    Instant startedAt,
    Instant finishedAt
) {}
