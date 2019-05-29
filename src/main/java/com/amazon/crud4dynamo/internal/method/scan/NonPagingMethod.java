package com.amazon.crud4dynamo.internal.method.scan;

import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;

public class NonPagingMethod implements AbstractMethod {

    private final Signature signature;
    private final Class<?> modelType;
    private final DynamoDBMapper dynamoDbMapper;
    private final DynamoDBMapperConfig mapperConfig;
    private final NonPagingExpressionFactory expressionFactory;

    public NonPagingMethod(
            final Signature signature,
            final Class<?> modelType,
            final DynamoDBMapper dynamoDbMapper,
            final DynamoDBMapperConfig mapperConfig) {
        this.signature = signature;
        this.modelType = modelType;
        this.dynamoDbMapper = dynamoDbMapper;
        this.mapperConfig = mapperConfig;
        expressionFactory = new NonPagingExpressionFactory(signature, modelType, dynamoDbMapper);
    }

    @Override
    public Signature getSignature() {
        return signature;
    }

    @Override
    public Object invoke(final Object... args) throws Throwable {
        return dynamoDbMapper.scan(modelType, expressionFactory.create(args), mapperConfig).iterator();
    }

    @Override
    public AbstractMethod bind(final Object target) {
        return this;
    }
}
