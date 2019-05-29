package com.amazon.crud4dynamo.internal.factory;

import com.amazon.crud4dynamo.annotation.Delete;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.factory.ChainedAbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.internal.method.DeleteMethod;

public class DeleteMethodFactory extends ChainedAbstractMethodFactory {
    public DeleteMethodFactory(final AbstractMethodFactory delegate) {
        super(delegate);
    }

    @Override
    public AbstractMethod create(Context context) {
        if (context.signature().invokable().isAnnotationPresent(Delete.class)) {
            return new DeleteMethod(
                    context.signature(), context.modelType(), context.mapper(), context.amazonDynamoDb(), context.mapperConfig());
        }
        return super.create(context);
    }
}
