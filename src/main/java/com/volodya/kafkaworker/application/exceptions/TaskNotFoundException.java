package com.volodya.kafkaworker.application.exceptions;

public final class TaskNotFoundException extends RuntimeException {

    public TaskNotFoundException(long id) {
        super("Task with id " + id + " is not found");
    }
}
