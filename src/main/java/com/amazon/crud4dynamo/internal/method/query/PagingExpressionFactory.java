package com.amazon.crud4dynamo.internal.method.query;

import com.amazon.crud4dynamo.exception.CrudForDynamoException;
import com.amazon.crud4dynamo.extension.PageRequest;
import com.amazon.crud4dynamo.internal.factory.ExpressionFactoryHelper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import java.util.Optional;

public class PagingExpressionFactory implements QueryExpressionFactory {
    private final QueryExpressionFactory expressionFactory;
    private final Class<?> tableType;
    private final DynamoDBMapper mapper;

    PagingExpressionFactory(final QueryExpressionFactory expressionFactory, final Class<?> tableType, final DynamoDBMapper mapper) {
        this.expressionFactory = expressionFactory;
        this.tableType = tableType;
        this.mapper = mapper;
    }

    @Override
    public DynamoDBQueryExpression create(final Object... args) {
        final Optional<PageRequest> pageRequest = ExpressionFactoryHelper.findPageRequest(args);
        if (!pageRequest.isPresent()) {
            throw new CrudForDynamoException("No PageRequest argument found");
        }
        return expressionFactory
                .create(args)
                .withExclusiveStartKey(ExpressionFactoryHelper.getLastEvaluatedKey(pageRequest.get(), mapper.getTableModel(tableType)))
                .withLimit(pageRequest.get().getLimit());
    }
}
