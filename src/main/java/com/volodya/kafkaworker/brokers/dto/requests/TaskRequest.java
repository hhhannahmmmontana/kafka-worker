package com.volodya.kafkaworker.brokers.dto.requests;

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

    @Positive
    long duration
) {}
