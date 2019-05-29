package com.amazon.crud4dynamo.internal.method.scan;

import com.amazon.crud4dynamo.extension.PageResult;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.ScanResultPage;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class PagingMethod implements AbstractMethod {

    private final Signature signature;
    private final Class<?> modelType;
    private final DynamoDBMapper dynamoDbMapper;
    private final DynamoDBMapperConfig mapperConfig;
    private final PagingExpressionFactory expressionFactory;

    public PagingMethod(
            final Signature signature,
            final Class<?> modelType,
            final DynamoDBMapper dynamoDbMapper,
            final DynamoDBMapperConfig mapperConfig) {
        this.signature = signature;
        this.modelType = modelType;
        this.dynamoDbMapper = dynamoDbMapper;
        this.mapperConfig = mapperConfig;
        expressionFactory =
                new PagingExpressionFactory(
                        new NonPagingExpressionFactory(signature, modelType, dynamoDbMapper), modelType, dynamoDbMapper);
    }

    @Override
    public Signature getSignature() {
        return signature;
    }

    @Override
    public Object invoke(final Object... args) throws Throwable {
        final ScanResultPage<?> result = dynamoDbMapper.scanPage(modelType, expressionFactory.create(args), mapperConfig);
        return PageResult.builder().items(result.getResults()).lastEvaluatedItem(getLastEvaluatedKey(result)).build();
    }

    private Object getLastEvaluatedKey(final ScanResultPage<?> result) {
        return Optional.ofNullable(result.getLastEvaluatedKey())
                .map((Function<Map, Object>) dynamoDbMapper.getTableModel(modelType)::unconvert)
                .orElse(null);
    }

    @Override
    public AbstractMethod bind(final Object target) {
        return this;
    }
}
