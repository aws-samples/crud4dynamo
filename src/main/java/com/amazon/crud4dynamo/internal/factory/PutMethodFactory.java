package com.amazon.crud4dynamo.internal.factory;

import com.amazon.crud4dynamo.annotation.Put;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.factory.ChainedAbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.internal.method.PutMethod;

public class PutMethodFactory extends ChainedAbstractMethodFactory {
    public PutMethodFactory(final AbstractMethodFactory delegate) {
        super(delegate);
    }

    @Override
    public AbstractMethod create(final Context context) {
        if (isAnnotatedWithPut(context)) {
            return new PutMethod(
                    context.signature(), context.modelType(), context.mapper(), context.amazonDynamoDb(), context.mapperConfig());
        }
        return super.create(context);
    }

    private static boolean isAnnotatedWithPut(final Context context) {
        return context.signature().invokable().isAnnotationPresent(Put.class);
    }
}
