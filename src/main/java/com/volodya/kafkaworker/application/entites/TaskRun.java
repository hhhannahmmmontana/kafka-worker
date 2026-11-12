package com.volodya.kafkaworker.application.entites;

import java.util.UUID;

public record TaskRun(long id, UUID executionId, String name, long duration) {}
