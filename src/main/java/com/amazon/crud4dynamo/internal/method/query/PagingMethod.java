package com.amazon.crud4dynamo.internal.method.query;

import com.amazon.crud4dynamo.extension.PageResult;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.QueryResultPage;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import lombok.NonNull;

public class PagingMethod implements AbstractMethod {
    private final Signature signature;
    private final Class<?> tableType;
    private final DynamoDBMapper mapper;
    private final DynamoDBMapperConfig mapperConfig;
    private final QueryExpressionFactory expressionFactory;

    public PagingMethod(
            @NonNull final Signature signature,
            @NonNull final Class<?> tableType,
            @NonNull final DynamoDBMapper mapper,
            final DynamoDBMapperConfig mapperConfig) {
        this.signature = signature;
        this.tableType = tableType;
        this.mapper = mapper;
        this.mapperConfig = mapperConfig;
        expressionFactory = new PagingExpressionFactory(new NonPagingExpressionFactory(signature, tableType, mapper), tableType, mapper);
    }

    @Override
    public Signature getSignature() {
        return signature;
    }

    @Override
    public AbstractMethod bind(final Object target) {
        return this;
    }

    @Override
    public Object invoke(final Object... args) throws Throwable {
        final QueryResultPage queryResultPage = mapper.queryPage(tableType, expressionFactory.create(args), mapperConfig);
        return PageResult.builder().items(queryResultPage.getResults()).lastEvaluatedItem(getLastEvaluatedItem(queryResultPage)).build();
    }

    private Object getLastEvaluatedItem(final QueryResultPage queryResultPage) {
        return Optional.ofNullable(queryResultPage.getLastEvaluatedKey())
                .map((Function<Map, Object>) mapper.getTableModel(tableType)::unconvert)
                .orElse(null);
    }
}
