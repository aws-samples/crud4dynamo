package com.amazon.crud4dynamo.internal.method;

import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.utility.ExceptionHelper;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import lombok.Getter;

public class ReflectiveMethod implements AbstractMethod {

    private final Object receiver;
    private final Method method;
    @Getter
    private final Signature signature;

    public ReflectiveMethod(final Object receiver, final Method method, final Signature signature) {
        this.receiver = receiver;
        this.method = method;
        this.signature = signature;
    }

    @Override
    public Object invoke(final Object... args) {
        try {
            return method.invoke(receiver, args);
        } catch (final IllegalAccessException | InvocationTargetException e) {
            throw ExceptionHelper.throwAsUnchecked(e);
        }
    }

    @Override
    public AbstractMethod bind(final Object target) {
        return this;
    }
}
