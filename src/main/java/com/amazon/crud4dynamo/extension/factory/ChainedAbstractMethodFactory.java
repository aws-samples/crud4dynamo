package com.amazon.crud4dynamo.extension.factory;

import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;

public abstract class ChainedAbstractMethodFactory implements AbstractMethodFactory {

    private final AbstractMethodFactory delegate;

    public ChainedAbstractMethodFactory(final AbstractMethodFactory delegate) {
        this.delegate = delegate;
    }

    @Override
    public AbstractMethod create(final Context context) {
        if (null != delegate) {
            return delegate.create(context);
        }
        return null;
    }
}
