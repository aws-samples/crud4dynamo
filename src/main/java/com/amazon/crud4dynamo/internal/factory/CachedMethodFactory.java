package com.amazon.crud4dynamo.internal.factory;

import com.amazon.crud4dynamo.annotation.Cached;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.factory.ChainedAbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.internal.method.CachedMethod;

public class CachedMethodFactory extends ChainedAbstractMethodFactory {
    public CachedMethodFactory(final AbstractMethodFactory delegate) {
        super(delegate);
    }

    @Override
    public AbstractMethod create(final Context context) {
        final AbstractMethod abstractMethod = super.create(context);
        return isAnnotatedWithCached(context) ? new CachedMethod(context.signature(), abstractMethod) : abstractMethod;
    }

    private boolean isAnnotatedWithCached(final Context context) {
        return context.signature().getAnnotation(Cached.class).isPresent();
    }
}
