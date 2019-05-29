package com.amazon.crud4dynamo.internal.factory;

import com.amazon.crud4dynamo.annotation.Update;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.factory.ChainedAbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.internal.method.UpdateMethod;

public class UpdateMethodFactory extends ChainedAbstractMethodFactory {
    public UpdateMethodFactory(final AbstractMethodFactory delegate) {
        super(delegate);
    }

    @Override
    public AbstractMethod create(final Context context) {
        if (isAnnotatedWithUpdate(context)) {
            return new UpdateMethod(
                    context.signature(), context.modelType(), context.mapper(), context.amazonDynamoDb(), context.mapperConfig());
        }
        return super.create(context);
    }

    private static boolean isAnnotatedWithUpdate(final Context context) {
        return context.signature().invokable().isAnnotationPresent(Update.class);
    }
}
