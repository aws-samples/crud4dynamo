package com.amazon.crud4dynamo.internal.factory;

import com.amazon.crud4dynamo.annotation.Scan;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.PageResult;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.factory.ChainedAbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.internal.method.scan.NonPagingMethod;
import com.amazon.crud4dynamo.internal.method.scan.PagingMethod;

public class ScanMethodFactory extends ChainedAbstractMethodFactory {
    public ScanMethodFactory(final AbstractMethodFactory delegate) {
        super(delegate);
    }

    @Override
    public AbstractMethod create(final Context context) {
        if (!isAnnotatedWithScan(context.signature())) {
            return super.create(context);
        }
        return requirePaging(context)
                ? new PagingMethod(context.signature(), context.modelType(), context.mapper(), context.mapperConfig())
                : new NonPagingMethod(context.signature(), context.modelType(), context.mapper(), context.mapperConfig());
    }

    private boolean requirePaging(final Context context) {
        return context.signature().invokable().getReturnType().isSubtypeOf(PageResult.class);
    }

    private boolean isAnnotatedWithScan(final Signature signature) {
        return signature.invokable().isAnnotationPresent(Scan.class);
    }
}
