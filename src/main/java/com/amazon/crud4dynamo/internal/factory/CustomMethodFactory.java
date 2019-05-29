package com.amazon.crud4dynamo.internal.factory;

import com.amazon.crud4dynamo.annotation.Custom;
import com.amazon.crud4dynamo.exception.CrudForDynamoException;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.factory.ChainedAbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.utility.Reflection;
import java.util.Optional;
import java.util.function.Supplier;

public class CustomMethodFactory extends ChainedAbstractMethodFactory {

    public CustomMethodFactory(final AbstractMethodFactory delegate) {
        super(delegate);
    }

    @Override
    public AbstractMethod create(final Context context) {
        final Optional<Custom> annotation = context.signature().getAnnotation(Custom.class);
        if (!annotation.isPresent()) {
            return super.create(context);
        }
        final AbstractMethodFactory factory =
                annotation.map(Custom::factoryClass).map(Reflection::newInstance).orElseThrow(getException(context));
        return factory.create(context);
    }

    private Supplier<CrudForDynamoException> getException(final Context context) {
        return () -> new CrudForDynamoException("Cannot create factory for method with signature:" + context.signature());
    }
}
