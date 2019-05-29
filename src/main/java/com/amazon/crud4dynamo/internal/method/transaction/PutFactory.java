package com.amazon.crud4dynamo.internal.method.transaction;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.exception.CrudForDynamoException;
import com.amazon.crud4dynamo.extension.Argument;
import com.amazon.crud4dynamo.internal.factory.ExpressionFactoryHelper;
import com.amazon.crud4dynamo.internal.parsing.ConditionExpressionParser;
import com.amazon.crud4dynamo.internal.parsing.ExpressionAttributesFactory;
import com.amazon.crud4dynamo.utility.DynamoDbHelper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperTableModel;
import com.amazonaws.services.dynamodbv2.model.Put;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;

public class PutFactory {
    private final com.amazon.crud4dynamo.annotation.transaction.Put putAnnotation;
    private final DynamoDBMapperTableModel tableModel;
    private final ExpressionAttributesFactory expressionAttributesFactory;

    public PutFactory(final com.amazon.crud4dynamo.annotation.transaction.Put putAnnotation, final DynamoDBMapperTableModel tableModel) {
        this.putAnnotation = putAnnotation;
        this.tableModel = tableModel;
        expressionAttributesFactory =
                new ExpressionAttributesFactory(new ConditionExpressionParser(putAnnotation.conditionExpression(), tableModel));
    }

    public Put create(final List<Argument> arguments) {
        return new Put()
                .withTableName(DynamoDbHelper.getTableName(putAnnotation.tableClass()))
                .withItem(tableModel.convert(findPutItemOrThrow(arguments)))
                .withConditionExpression(ExpressionFactoryHelper.toNullIfBlank(putAnnotation.conditionExpression()))
                .withExpressionAttributeNames(expressionAttributesFactory.newExpressionAttributeNames(arguments))
                .withExpressionAttributeValues(expressionAttributesFactory.newExpressionAttributeValues(arguments))
                .withReturnValuesOnConditionCheckFailure(putAnnotation.returnValuesOnConditionCheckFailure());
    }

    private Object findPutItemOrThrow(final List<Argument> arguments) {
        return arguments
                .stream()
                .filter(a -> a.getParameter().getAnnotation(Param.class) != null)
                .filter(a -> a.getParameter().getAnnotation(Param.class).value().equals(putAnnotation.item()))
                .findFirst()
                .map(Argument::getValue)
                .orElseThrow(() -> new NoPutItemException(putAnnotation));
    }

    @VisibleForTesting
    static class NoPutItemException extends CrudForDynamoException {
        private NoPutItemException(final com.amazon.crud4dynamo.annotation.transaction.Put annotation) {
            super(String.format("Cannot find parameter annotated with @Param(%s)", annotation.item()));
        }
    }
}
