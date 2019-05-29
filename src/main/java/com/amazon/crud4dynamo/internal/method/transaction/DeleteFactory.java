package com.amazon.crud4dynamo.internal.method.transaction;

import com.amazon.crud4dynamo.extension.Argument;
import com.amazon.crud4dynamo.internal.factory.ExpressionFactoryHelper;
import com.amazon.crud4dynamo.internal.parsing.ConditionExpressionParser;
import com.amazon.crud4dynamo.internal.parsing.ExpressionAttributesFactory;
import com.amazon.crud4dynamo.internal.utility.KeyAttributeConstructor;
import com.amazon.crud4dynamo.utility.DynamoDbHelper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperTableModel;
import com.amazonaws.services.dynamodbv2.model.Delete;
import java.util.List;

public class DeleteFactory {
    private final com.amazon.crud4dynamo.annotation.transaction.Delete deleteAnnotation;
    private final DynamoDBMapperTableModel<?> tableModel;
    private final KeyAttributeConstructor keyAttributeConstructor;
    private final ExpressionAttributesFactory expressionAttributesFactory;

    public DeleteFactory(
            final com.amazon.crud4dynamo.annotation.transaction.Delete deleteAnnotation, final DynamoDBMapperTableModel<?> tableModel) {
        this.deleteAnnotation = deleteAnnotation;
        this.tableModel = tableModel;
        keyAttributeConstructor = new KeyAttributeConstructor(deleteAnnotation.keyExpression(), tableModel);
        expressionAttributesFactory =
                new ExpressionAttributesFactory(new ConditionExpressionParser(deleteAnnotation.conditionExpression(), tableModel));
    }

    public Delete create(final List<Argument> arguments) {
        return new Delete()
                .withTableName(DynamoDbHelper.getTableName(deleteAnnotation.tableClass()))
                .withKey(keyAttributeConstructor.create(arguments))
                .withConditionExpression(ExpressionFactoryHelper.toNullIfBlank(deleteAnnotation.conditionExpression()))
                .withExpressionAttributeNames(expressionAttributesFactory.newExpressionAttributeNames(arguments))
                .withExpressionAttributeValues(expressionAttributesFactory.newExpressionAttributeValues(arguments))
                .withReturnValuesOnConditionCheckFailure(deleteAnnotation.returnValuesOnConditionCheckFailure());
    }
}
