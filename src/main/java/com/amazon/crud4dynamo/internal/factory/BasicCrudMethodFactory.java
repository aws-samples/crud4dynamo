package com.amazon.crud4dynamo.internal.factory;

import com.amazon.crud4dynamo.crudinterface.CompositeKeyCrud;
import com.amazon.crud4dynamo.crudinterface.DynamoDbCrud;
import com.amazon.crud4dynamo.crudinterface.SimpleKeyCrud;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.factory.ChainedAbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.internal.CompositeKeyCrudImpl;
import com.amazon.crud4dynamo.internal.SimpleKeyCrudImpl;
import com.amazon.crud4dynamo.internal.method.ReflectiveMethod;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class BasicCrudMethodFactory extends ChainedAbstractMethodFactory {
    private final Map<Class<?>, Map<String, Method>> methodMapCache = new HashMap<>();
    private final Map<Class<? extends DynamoDbCrud>, Function<Context, DynamoDbCrud>> daoFactory;

    public BasicCrudMethodFactory(final AbstractMethodFactory delegate) {
        super(delegate);
        daoFactory =
                ImmutableMap.of(
                        SimpleKeyCrud.class,
                        context -> new SimpleKeyCrudImpl<>(context.mapper(), context.mapperConfig(), context.modelType()),
                        CompositeKeyCrud.class,
                        context -> new CompositeKeyCrudImpl<>(context.mapper(), context.mapperConfig(), context.modelType()));
    }

    @Override
    public AbstractMethod create(final Context context) {
        final Map<String, Method> map = getMethodMap(context);
        final DynamoDbCrud dao = getDao(context);
        return Optional.ofNullable(map.get(context.signature().toString()))
                .<AbstractMethod>map(delegateMethod -> new ReflectiveMethod(dao, delegateMethod, context.signature()))
                .orElseGet(() -> super.create(context));
    }

    private Map<String, Method> getMethodMap(final Context context) {
        return methodMapCache.computeIfAbsent(
                context.interfaceType(), key -> Arrays.stream(getMethods(context)).collect(toMethodMap(context)));
    }

    private Method[] getMethods(final Context context) {
        return Optional.of(context.interfaceType())
                .filter(SimpleKeyCrud.class::isAssignableFrom)
                .map(key -> SimpleKeyCrud.class.getMethods())
                .orElseGet(CompositeKeyCrud.class::getMethods);
    }

    private Collector<Method, ?, Map<String, Method>> toMethodMap(final Context context) {
        return Collectors.toMap(m -> Signature.resolve(m, context.interfaceType()).toString(), Function.identity());
    }

    private DynamoDbCrud getDao(final Context context) {
        return daoFactory.get(getCrudType(context)).apply(context);
    }

    private Class<? extends DynamoDbCrud> getCrudType(final Context context) {
        return Optional.of(context.interfaceType())
                .filter(SimpleKeyCrud.class::isAssignableFrom)
                .<Class<? extends DynamoDbCrud>>map(key -> SimpleKeyCrud.class)
                .orElse(CompositeKeyCrud.class);
    }
}
