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

package com.amazon.crud4dynamo.internal.method.query;

import com.amazon.crud4dynamo.annotation.Query;
import com.amazon.crud4dynamo.extension.Argument;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.internal.factory.ExpressionFactoryHelper;
import com.amazon.crud4dynamo.internal.parsing.ConditionExpressionParser;
import com.amazon.crud4dynamo.internal.parsing.ExpressionAttributesFactory;
import com.amazon.crud4dynamo.internal.parsing.KeyConditionExpressionParser;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import lombok.NonNull;

public class NonPagingExpressionFactory implements QueryExpressionFactory {
    private final Signature signature;
    private final Query query;
    private final DynamoDBMapper mapper;
    private final Class<?> tableType;
    private final ExpressionAttributesFactory expressionAttributesFactory;

    NonPagingExpressionFactory(
            @NonNull final Signature signature, @NonNull final Class<?> tableType, @NonNull final DynamoDBMapper mapper) {
        this.signature = signature;
        this.mapper = mapper;
        this.tableType = tableType;
        query =
                Preconditions.checkNotNull(
                        signature.invokable().getAnnotation(Query.class),
                        String.format("Method with signature '%s' is not annotated with Query", signature));
        expressionAttributesFactory =
                new ExpressionAttributesFactory(
                        new KeyConditionExpressionParser(query.keyCondition(), mapper.getTableModel(tableType)),
                        new ConditionExpressionParser(query.filter(), mapper.getTableModel(tableType)));
    }

    @Override
    public DynamoDBQueryExpression create(final Object... args) {
        final List<Argument> argList = Argument.newList(signature.parameters(), Arrays.asList(args));
        return new DynamoDBQueryExpression()
                .withKeyConditionExpression(ExpressionFactoryHelper.toNullIfBlank(query.keyCondition()))
                .withFilterExpression(ExpressionFactoryHelper.toNullIfBlank(query.filter()))
                .withIndexName(ExpressionFactoryHelper.toNullIfBlank(query.index()))
                .withExpressionAttributeNames(expressionAttributesFactory.newExpressionAttributeNames(argList))
                .withExpressionAttributeValues(expressionAttributesFactory.newExpressionAttributeValues(argList))
                .withConsistentRead(query.consistentRead())
                .withScanIndexForward(query.scanIndexForward());
    }
}
