package com.amazon.crud4dynamo.internal.method.transaction;

import com.amazon.crud4dynamo.extension.Argument;
import com.amazon.crud4dynamo.internal.factory.ExpressionFactoryHelper;
import com.amazon.crud4dynamo.internal.parsing.ConditionExpressionParser;
import com.amazon.crud4dynamo.internal.parsing.ExpressionAttributesFactory;
import com.amazon.crud4dynamo.internal.parsing.UpdateExpressionParser;
import com.amazon.crud4dynamo.internal.utility.KeyAttributeConstructor;
import com.amazon.crud4dynamo.utility.DynamoDbHelper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperTableModel;
import com.amazonaws.services.dynamodbv2.model.Update;
import java.util.List;

public class UpdateFactory {
    private final com.amazon.crud4dynamo.annotation.transaction.Update updateAnnotation;
    private final DynamoDBMapperTableModel tableModel;
    private final KeyAttributeConstructor keyAttributeConstructor;
    private final ExpressionAttributesFactory expressionAttributesFactory;

    public UpdateFactory(
            final com.amazon.crud4dynamo.annotation.transaction.Update updateAnnotation, final DynamoDBMapperTableModel tableModel) {
        this.updateAnnotation = updateAnnotation;
        this.tableModel = tableModel;
        keyAttributeConstructor = new KeyAttributeConstructor(updateAnnotation.keyExpression(), tableModel);
        expressionAttributesFactory =
                new ExpressionAttributesFactory(
                        new UpdateExpressionParser(updateAnnotation.updateExpression(), tableModel),
                        new ConditionExpressionParser(updateAnnotation.conditionExpression(), tableModel));
    }

    public Update create(final List<Argument> arguments) {
        return new Update()
                .withTableName(DynamoDbHelper.getTableName(updateAnnotation.tableClass()))
                .withKey(keyAttributeConstructor.create(arguments))
                .withUpdateExpression(ExpressionFactoryHelper.toNullIfBlank(updateAnnotation.updateExpression()))
                .withConditionExpression(ExpressionFactoryHelper.toNullIfBlank(updateAnnotation.conditionExpression()))
                .withExpressionAttributeNames(expressionAttributesFactory.newExpressionAttributeNames(arguments))
                .withExpressionAttributeValues(expressionAttributesFactory.newExpressionAttributeValues(arguments))
                .withReturnValuesOnConditionCheckFailure(updateAnnotation.returnValuesOnConditionCheckFailure());
    }
}
