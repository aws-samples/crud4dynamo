package com.amazon.crud4dynamo.internal.factory;

import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.factory.ChainedAbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.internal.method.DefaultMethod;

/** Create method handle for default method declared in the interface. */
public class DefaultMethodFactory extends ChainedAbstractMethodFactory {

    public DefaultMethodFactory(final AbstractMethodFactory delegate) {
        super(delegate);
    }

    @Override
    public AbstractMethod create(final Context context) {
        if (!context.method().isDefault()) {
            return super.create(context);
        }
        return new DefaultMethod(context.method(), Signature.resolve(context.method(), context.interfaceType()));
    }
}
