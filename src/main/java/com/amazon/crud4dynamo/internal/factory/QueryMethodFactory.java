package com.amazon.crud4dynamo.internal.factory;

import com.amazon.crud4dynamo.annotation.Query;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.PageResult;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.factory.ChainedAbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.internal.method.query.NonPagingMethod;
import com.amazon.crud4dynamo.internal.method.query.PagingMethod;

public class QueryMethodFactory extends ChainedAbstractMethodFactory {

    public QueryMethodFactory(final AbstractMethodFactory delegate) {
        super(delegate);
    }

    @Override
    public AbstractMethod create(final Context context) {
        if (!isAnnotatedWithQuery(context.signature())) {
            return super.create(context);
        }
        return isPagingQuery(context)
                ? new PagingMethod(context.signature(), context.modelType(), context.mapper(), context.mapperConfig())
                : new NonPagingMethod(context.signature(), context.modelType(), context.mapper(), context.mapperConfig());
    }

    private boolean isPagingQuery(final Context context) {
        return context.signature().invokable().getReturnType().isSubtypeOf(PageResult.class);
    }

    private boolean isAnnotatedWithQuery(final Signature signature) {
        return signature.invokable().isAnnotationPresent(Query.class);
    }
}
