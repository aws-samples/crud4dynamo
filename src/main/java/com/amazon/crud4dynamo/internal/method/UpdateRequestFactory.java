/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.crud4dynamo.internal.method;

import com.amazon.crud4dynamo.annotation.Update;
import com.amazon.crud4dynamo.extension.Argument;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.internal.factory.ExpressionFactoryHelper;
import com.amazon.crud4dynamo.internal.parsing.ConditionExpressionParser;
import com.amazon.crud4dynamo.internal.parsing.ExpressionAttributesFactory;
import com.amazon.crud4dynamo.internal.parsing.UpdateExpressionParser;
import com.amazon.crud4dynamo.internal.utility.KeyAttributeConstructor;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperTableModel;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;

public class UpdateRequestFactory {
    private final Signature signature;
    private final Class<?> modelClass;
    private final DynamoDBMapper dynamoDbMapper;
    private final Update update;
    private final KeyAttributeConstructor keyAttributeConstructor;
    private final ExpressionAttributesFactory expressionAttributesFactory;

    public UpdateRequestFactory(final Signature signature, final Class<?> modelClass, final DynamoDBMapper dynamoDbMapper) {
        this.signature = signature;
        this.modelClass = modelClass;
        this.dynamoDbMapper = dynamoDbMapper;
        update =
                Preconditions.checkNotNull(
                        signature.invokable().getAnnotation(Update.class),
                        String.format("Method with signature '%s' is not annotated with Update", signature));
        final DynamoDBMapperTableModel<?> tableModel = dynamoDbMapper.getTableModel(modelClass);
        keyAttributeConstructor = new KeyAttributeConstructor(update.keyExpression(), tableModel);
        expressionAttributesFactory =
                new ExpressionAttributesFactory(
                        new UpdateExpressionParser(update.updateExpression(), tableModel),
                        new ConditionExpressionParser(update.conditionExpression(), tableModel));
    }

    public UpdateItemRequest create(final Object... args) {
        final List<Argument> argList = Argument.newList(signature.parameters(), Arrays.asList(args));
        return new UpdateItemRequest()
                .withKey(keyAttributeConstructor.create(argList))
                .withTableName(ExpressionFactoryHelper.getTableName(modelClass))
                .withUpdateExpression(update.updateExpression())
                .withConditionExpression(ExpressionFactoryHelper.toNullIfBlank(update.conditionExpression()))
                .withExpressionAttributeNames(expressionAttributesFactory.newExpressionAttributeNames(argList))
                .withExpressionAttributeValues(expressionAttributesFactory.newExpressionAttributeValues(argList))
                .withReturnValues(update.returnValue());
    }
}
