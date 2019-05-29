package com.amazon.crud4dynamo.internal.factory;

import com.amazon.crud4dynamo.annotation.transaction.ConditionCheck;
import com.amazon.crud4dynamo.annotation.transaction.Delete;
import com.amazon.crud4dynamo.annotation.transaction.Put;
import com.amazon.crud4dynamo.annotation.transaction.Update;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.factory.ChainedAbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.internal.method.transaction.TransactionWriteMethod;
import com.google.common.collect.ImmutableSet;
import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Set;

public class TransactionWriteMethodFactory extends ChainedAbstractMethodFactory {
    private static final Set<Class<? extends Annotation>> TRANSACTION_WRITE_ANNOTATIONS =
            ImmutableSet.of(ConditionCheck.class, Put.class, Update.class, Delete.class);

    public TransactionWriteMethodFactory(final AbstractMethodFactory delegate) {
        super(delegate);
    }

    @Override
    public AbstractMethod create(final Context context) {
        final Signature signature = context.signature();
        if (TRANSACTION_WRITE_ANNOTATIONS.stream().map(signature::getAnnotationsByType).allMatch(List::isEmpty)) {
            return super.create(context);
        }
        return new TransactionWriteMethod(context.amazonDynamoDb(), context.mapper(), signature);
    }
}
