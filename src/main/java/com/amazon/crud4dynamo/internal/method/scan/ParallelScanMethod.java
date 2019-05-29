package com.amazon.crud4dynamo.internal.method.scan;

import com.amazon.crud4dynamo.annotation.Parallel;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;

public class ParallelScanMethod implements AbstractMethod {

    private final Signature signature;
    private final Class<?> modelType;
    private final DynamoDBMapper mapper;
    private final DynamoDBMapperConfig mapperConfig;
    private final NonPagingExpressionFactory expressionFactory;

    public ParallelScanMethod(
            final Signature signature, final Class<?> modelType, final DynamoDBMapper mapper, final DynamoDBMapperConfig mapperConfig) {
        this.signature = signature;
        this.modelType = modelType;
        this.mapper = mapper;
        this.mapperConfig = mapperConfig;
        expressionFactory = new NonPagingExpressionFactory(signature, modelType, mapper);
    }

    @Override
    public Signature getSignature() {
        return signature;
    }

    @Override
    public Object invoke(final Object... args) throws Throwable {
        return mapper.parallelScan(modelType, expressionFactory.create(args), getTotalSegments()).iterator();
    }

    private Integer getTotalSegments() {
        return signature.getAnnotation(Parallel.class).map(Parallel::totalSegments).get();
    }

    @Override
    public AbstractMethod bind(final Object target) {
        return this;
    }
}
