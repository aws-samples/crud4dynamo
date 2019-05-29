package com.amazon.crud4dynamo.internal.method.query;

import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import lombok.NonNull;

public class NonPagingMethod implements AbstractMethod {
    private final Signature signature;
    private final Class<?> tableType;
    private final DynamoDBMapper mapper;
    private final QueryExpressionFactory expressionFactory;
    private final DynamoDBMapperConfig mapperConfig;

    public NonPagingMethod(
            @NonNull final Signature signature,
            @NonNull final Class<?> tableType,
            @NonNull final DynamoDBMapper mapper,
            final DynamoDBMapperConfig mapperConfig) {
        this.signature = signature;
        this.tableType = tableType;
        this.mapper = mapper;
        this.mapperConfig = mapperConfig;
        expressionFactory = new NonPagingExpressionFactory(signature, tableType, mapper);
    }

    @Override
    public AbstractMethod bind(final Object target) {
        return this;
    }

    @Override
    public Signature getSignature() {
        return signature;
    }

    @Override
    public Object invoke(final Object... args) throws Throwable {
        return mapper.query(tableType, expressionFactory.create(args), mapperConfig).iterator();
    }
}
