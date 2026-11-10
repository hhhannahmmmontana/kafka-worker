package com.volodya.kafkaworker.infrastructure.consuming.dto.requests;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.util.UUID;

public record TaskRequest(
    @NotNull
    UUID eventId,

    @NotBlank
    @Size(max = 255)
    String name,

    @NotNull
    @Positive
    Long duration
) {}
