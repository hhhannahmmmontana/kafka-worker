package com.volodya.kafkaworker.application.exceptions;

public final class IllegalTaskStateException extends RuntimeException {

    public IllegalTaskStateException(String message) {
        super(message);
    }
}
