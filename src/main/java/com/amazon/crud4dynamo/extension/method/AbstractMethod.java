package com.amazon.crud4dynamo.extension.method;

import com.amazon.crud4dynamo.extension.Signature;

/** An abstraction of an invokable method handle. */
public interface AbstractMethod {

    Signature getSignature();

    Object invoke(final Object... args) throws Throwable;

    AbstractMethod bind(final Object target);
}
