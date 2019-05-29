package com.amazon.crud4dynamo.internal.factory;

import com.amazon.crud4dynamo.exception.CrudForDynamoException;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.factory.ChainedAbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;

public class ThrowingMethodFactory extends ChainedAbstractMethodFactory {
    public ThrowingMethodFactory(final AbstractMethodFactory delegate) {
        super(null);
    }

    @Override
    public AbstractMethod create(final Context context) {
        final String msg =
                String.format(
                        "No method factory can respond to method with signature %s%n, interface type %s%n, and raw method %s",
                        context.signature(), context.interfaceType(), context.method());
        throw new CrudForDynamoException(msg);
    }
}
