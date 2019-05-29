package com.amazon.crud4dynamo.internal.method.transaction;

import com.amazon.crud4dynamo.extension.Argument;
import com.amazon.crud4dynamo.internal.parsing.ConditionExpressionParser;
import com.amazon.crud4dynamo.internal.parsing.ExpressionAttributesFactory;
import com.amazon.crud4dynamo.internal.utility.KeyAttributeConstructor;
import com.amazon.crud4dynamo.utility.DynamoDbHelper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperTableModel;
import com.amazonaws.services.dynamodbv2.model.ConditionCheck;
import java.util.List;

public class ConditionCheckFactory {
    private final com.amazon.crud4dynamo.annotation.transaction.ConditionCheck conditionCheckAnnotation;
    private final KeyAttributeConstructor keyAttributeConstructor;
    private final ExpressionAttributesFactory expressionAttributesFactory;

    public ConditionCheckFactory(
            final com.amazon.crud4dynamo.annotation.transaction.ConditionCheck conditionCheckAnnotation,
            final DynamoDBMapperTableModel<?> tableModel) {
        this.conditionCheckAnnotation = conditionCheckAnnotation;
        keyAttributeConstructor = new KeyAttributeConstructor(conditionCheckAnnotation.keyExpression(), tableModel);
        expressionAttributesFactory =
                new ExpressionAttributesFactory(new ConditionExpressionParser(conditionCheckAnnotation.conditionExpression(), tableModel));
    }

    public ConditionCheck create(final List<Argument> arguments) {
        return new ConditionCheck()
                .withTableName(DynamoDbHelper.getTableName(conditionCheckAnnotation.tableClass()))
                .withConditionExpression(conditionCheckAnnotation.conditionExpression())
                .withExpressionAttributeNames(expressionAttributesFactory.newExpressionAttributeNames(arguments))
                .withExpressionAttributeValues(expressionAttributesFactory.newExpressionAttributeValues(arguments))
                .withKey(keyAttributeConstructor.create(arguments))
                .withReturnValuesOnConditionCheckFailure(conditionCheckAnnotation.returnValuesOnConditionCheckFailure());
    }
}
