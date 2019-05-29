package com.amazon.crud4dynamo.internal.method.scan;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;

public interface ScanExpressionFactory {
    DynamoDBScanExpression create(final Object... args);
}
