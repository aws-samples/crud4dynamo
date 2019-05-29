package com.amazon.crud4dynamo.internal.method.scan;

import com.amazon.crud4dynamo.annotation.Scan;
import com.amazon.crud4dynamo.extension.Argument;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.internal.factory.ExpressionFactoryHelper;
import com.amazon.crud4dynamo.internal.parsing.ConditionExpressionParser;
import com.amazon.crud4dynamo.internal.parsing.ExpressionAttributesFactory;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import lombok.NonNull;

public class NonPagingExpressionFactory implements ScanExpressionFactory {
    private final Signature signature;
    private final Class<?> tableType;
    private final DynamoDBMapper mapper;
    private final Scan annotation;
    private final ExpressionAttributesFactory expressionAttributesFactory;

    NonPagingExpressionFactory(
            @NonNull final Signature signature, @NonNull final Class<?> tableType, @NonNull final DynamoDBMapper mapper) {
        this.signature = signature;
        this.tableType = tableType;
        this.mapper = mapper;
        annotation =
                Preconditions.checkNotNull(
                        signature.invokable().getAnnotation(Scan.class),
                        String.format("Method with signature '%s' is not annotated with Scan.", signature));
        expressionAttributesFactory =
                new ExpressionAttributesFactory(new ConditionExpressionParser(annotation.filter(), mapper.getTableModel(tableType)));
    }

    @Override
    public DynamoDBScanExpression create(final Object... args) {
        final List<Argument> argList = Argument.newList(signature.parameters(), Arrays.asList(args));
        return new DynamoDBScanExpression()
                .withConsistentRead(annotation.consistentRead())
                .withExpressionAttributeNames(expressionAttributesFactory.newExpressionAttributeNames(argList))
                .withExpressionAttributeValues(expressionAttributesFactory.newExpressionAttributeValues(argList))
                .withIndexName(ExpressionFactoryHelper.toNullIfBlank(annotation.index()))
                .withFilterExpression(ExpressionFactoryHelper.toNullIfBlank(annotation.filter()));
    }
}
