package com.amazon.crud4dynamo.exception;

public class CrudForDynamoException extends RuntimeException {
    public CrudForDynamoException(final String message) {
        super(message);
    }

    public CrudForDynamoException(final Throwable throwable) {
        super(throwable);
    }

    public CrudForDynamoException(final String message, final Throwable throwable) {
        super(message, throwable);
    }
}
