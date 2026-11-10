package com.volodya.kafkaworker.application.exceptions;

public final class InvalidTaskMessageException extends RuntimeException {

    public InvalidTaskMessageException(String message) {
        super(message);
    }
}
