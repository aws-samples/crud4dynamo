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

package com.amazon.crud4dynamo.internal.method.transaction;

import com.amazon.crud4dynamo.extension.Argument;
import com.amazon.crud4dynamo.internal.parsing.ExpressionAttributesFactory;
import com.amazon.crud4dynamo.internal.parsing.ProjectionExpressionParser;
import com.amazon.crud4dynamo.internal.utility.KeyAttributeConstructor;
import com.amazon.crud4dynamo.utility.DynamoDbHelper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperTableModel;
import com.amazonaws.services.dynamodbv2.model.Get;
import java.util.List;

public class GetFactory {
    private final com.amazon.crud4dynamo.annotation.transaction.Get getAnnotation;
    private final DynamoDBMapperTableModel<?> tableModel;
    private final KeyAttributeConstructor keyAttributeConstructor;
    private final ExpressionAttributesFactory expressionAttributesFactory;

    public GetFactory(final com.amazon.crud4dynamo.annotation.transaction.Get getAnnotation, final DynamoDBMapperTableModel<?> tableModel) {
        this.getAnnotation = getAnnotation;
        this.tableModel = tableModel;
        keyAttributeConstructor = new KeyAttributeConstructor(getAnnotation.keyExpression(), tableModel);
        expressionAttributesFactory = new ExpressionAttributesFactory(new ProjectionExpressionParser(getAnnotation.projectionExpression()));
    }

    public Get create(final List<Argument> arguments) {
        return new Get()
                .withTableName(DynamoDbHelper.getTableName(getAnnotation.tableClass()))
                .withExpressionAttributeNames(expressionAttributesFactory.newExpressionAttributeNames(arguments))
                .withKey(keyAttributeConstructor.create(arguments))
                .withProjectionExpression(getAnnotation.projectionExpression());
    }
}
