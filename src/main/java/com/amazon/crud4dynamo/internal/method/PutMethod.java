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

import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperTableModel;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import java.util.Optional;
import lombok.NonNull;

public class PutMethod implements AbstractMethod {
    private final Signature signature;
    private final Class<?> modelType;
    private final DynamoDBMapper mapper;
    private final AmazonDynamoDB amazonDynamoDb;
    private final DynamoDBMapperConfig mapperConfig;
    private final PutRequestFactory putRequestFactory;
    private final DynamoDBMapperTableModel<?> tableModel;

    public PutMethod(
            @NonNull final Signature signature,
            @NonNull final Class<?> modelType,
            @NonNull final DynamoDBMapper mapper,
            @NonNull final AmazonDynamoDB amazonDynamoDb,
            @NonNull final DynamoDBMapperConfig mapperConfig) {
        this.signature = signature;
        this.modelType = modelType;
        this.mapper = mapper;
        this.amazonDynamoDb = amazonDynamoDb;
        this.mapperConfig = mapperConfig;
        tableModel = mapper.getTableModel(modelType);
        putRequestFactory = new PutRequestFactory(signature, modelType, mapper);
    }

    @Override
    public Signature getSignature() {
        return signature;
    }

    @Override
    public Object invoke(final Object... args) throws Throwable {
        final PutItemRequest putItemRequest = putRequestFactory.create(args);
        final PutItemResult putItemResult = amazonDynamoDb.putItem(putItemRequest);
        return Optional.ofNullable(putItemResult.getAttributes()).map(tableModel::unconvert).orElse(null);
    }

    @Override
    public AbstractMethod bind(final Object target) {
        return this;
    }
}
