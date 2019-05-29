package com.amazon.crud4dynamo.exception;

public class MethodNotFoundException extends CrudForDynamoException {
    public MethodNotFoundException(final String message) {
        super(message);
    }
}
