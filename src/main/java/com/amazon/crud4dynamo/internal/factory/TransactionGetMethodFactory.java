package com.amazon.crud4dynamo.internal.factory;

import com.amazon.crud4dynamo.annotation.transaction.Get;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.factory.ChainedAbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.internal.method.transaction.TransactionGetMethod;
import com.google.common.collect.ImmutableSet;
import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Set;

public class TransactionGetMethodFactory extends ChainedAbstractMethodFactory {
    private static final Set<Class<? extends Annotation>> TRANSACTION_GET_ANNOTATIONS = ImmutableSet.of(Get.class);

    public TransactionGetMethodFactory(final AbstractMethodFactory delegate) {
        super(delegate);
    }

    @Override
    public AbstractMethod create(final Context context) {
        final Signature signature = context.signature();
        if (TRANSACTION_GET_ANNOTATIONS.stream().map(signature::getAnnotationsByType).allMatch(List::isEmpty)) {
            return super.create(context);
        }
        return new TransactionGetMethod(context.amazonDynamoDb(), context.mapper(), signature);
    }
}
