package com.amazon.crud4dynamo.internal.method.query;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;

public interface QueryExpressionFactory {
    DynamoDBQueryExpression create(final Object... args);
}
