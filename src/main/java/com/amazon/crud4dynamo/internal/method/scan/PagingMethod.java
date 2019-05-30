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

package com.amazon.crud4dynamo.internal.method.scan;

import com.amazon.crud4dynamo.extension.PageResult;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.ScanResultPage;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class PagingMethod implements AbstractMethod {

    private final Signature signature;
    private final Class<?> modelType;
    private final DynamoDBMapper dynamoDbMapper;
    private final DynamoDBMapperConfig mapperConfig;
    private final PagingExpressionFactory expressionFactory;

    public PagingMethod(
            final Signature signature,
            final Class<?> modelType,
            final DynamoDBMapper dynamoDbMapper,
            final DynamoDBMapperConfig mapperConfig) {
        this.signature = signature;
        this.modelType = modelType;
        this.dynamoDbMapper = dynamoDbMapper;
        this.mapperConfig = mapperConfig;
        expressionFactory =
                new PagingExpressionFactory(
                        new NonPagingExpressionFactory(signature, modelType, dynamoDbMapper), modelType, dynamoDbMapper);
    }

    @Override
    public Signature getSignature() {
        return signature;
    }

    @Override
    public Object invoke(final Object... args) throws Throwable {
        final ScanResultPage<?> result = dynamoDbMapper.scanPage(modelType, expressionFactory.create(args), mapperConfig);
        return PageResult.builder().items(result.getResults()).lastEvaluatedItem(getLastEvaluatedKey(result)).build();
    }

    private Object getLastEvaluatedKey(final ScanResultPage<?> result) {
        return Optional.ofNullable(result.getLastEvaluatedKey())
                .map((Function<Map, Object>) dynamoDbMapper.getTableModel(modelType)::unconvert)
                .orElse(null);
    }

    @Override
    public AbstractMethod bind(final Object target) {
        return this;
    }
}
