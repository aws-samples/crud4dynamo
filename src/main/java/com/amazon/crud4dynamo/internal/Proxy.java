package com.amazon.crud4dynamo.internal;

import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.google.common.reflect.AbstractInvocationHandler;
import com.google.common.reflect.Reflection;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Proxy<T> {
    private final Class<T> interfaceType;
    private final Function<Method, AbstractMethod> methodFunction;

    public Proxy(final Class<T> interfaceType, final Function<Method, AbstractMethod> methodFunction) {
        this.interfaceType = interfaceType;
        this.methodFunction = methodFunction;
    }

    public T create() {
        final Map<Method, AbstractMethod> dispatchMapping =
                Arrays.stream(interfaceType.getMethods()).collect(Collectors.toMap(Function.identity(), methodFunction));
        return Reflection.newProxy(
                interfaceType,
                new AbstractInvocationHandler() {
                    @Override
                    protected Object handleInvocation(final Object o, final Method method, final Object[] objects) throws Throwable {
                        try {
                            return dispatchMapping.get(method).bind(o).invoke(objects);
                        } catch (final InvocationTargetException e) {
                            throw e.getTargetException();
                        }
                    }
                });
    }
}
