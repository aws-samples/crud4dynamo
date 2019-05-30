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

package com.amazon.crud4dynamo.internal;

import com.amazon.crud4dynamo.crudinterface.CompositeKeyCrud;
import com.amazon.crud4dynamo.exception.CrudForDynamoException;
import com.amazon.crud4dynamo.extension.PageRequest;
import com.amazon.crud4dynamo.extension.PageResult;
import com.amazon.crud4dynamo.internal.utility.DynamoDbMapperConfigHelper;
import com.amazon.crud4dynamo.utility.Reflection;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.QueryResultPage;
import java.util.Iterator;
import java.util.Optional;

/**
 * This class provides implementation only for operations declared in CompositeKeyCrud interface.
 *
 * @param <H> HashKey Generic Type Parameter
 * @param <R> RangeKey Generic Type Parameter
 * @param <M> Model Generic Type Parameter
 */
public class CompositeKeyCrudImpl<H, R, M> extends DynamoDbCrudBase<M> implements CompositeKeyCrud<H, R, M> {

    public CompositeKeyCrudImpl(final DynamoDBMapper dynamoDbMapper, final DynamoDBMapperConfig mapperConfig, final Class<M> modelClass) {
        super(dynamoDbMapper, mapperConfig, modelClass);
    }

    @Override
    public void deleteBy(final H hashKey, final R rangeKey) throws CrudForDynamoException {
        final M model = Reflection.newInstance(modelClass);
        tableModel.hashKey().set(model, hashKey);
        tableModel.rangeKey().set(model, rangeKey);
        dynamoDbMapper.delete(model, mapperConfig);
    }

    @Override
    public Optional<M> findBy(final H hashKey, final R rangeKey) throws CrudForDynamoException {
        return Optional.ofNullable(dynamoDbMapper.load(modelClass, hashKey, rangeKey, mapperConfig));
    }

    @Override
    public Iterator<M> groupBy(final H hashKey) throws CrudForDynamoException {
        final M model = Reflection.newInstance(modelClass);
        tableModel.hashKey().set(model, hashKey);
        return dynamoDbMapper
                .query(
                        modelClass,
                        new DynamoDBQueryExpression<M>().withHashKeyValues(model),
                        DynamoDbMapperConfigHelper.override(
                                mapperConfig, DynamoDBMapperConfig.PaginationLoadingStrategy.ITERATION_ONLY.config()))
                .iterator();
    }

    @Override
    public PageResult<M> groupBy(final H hashKey, final PageRequest<M> pageRequest) throws CrudForDynamoException {
        final M model = Reflection.newInstance(modelClass);
        tableModel.hashKey().set(model, hashKey);
        final DynamoDBQueryExpression<M> expression =
                new DynamoDBQueryExpression<M>()
                        .withLimit(pageRequest.getLimit())
                        .withExclusiveStartKey(
                                Optional.ofNullable(pageRequest.getExclusiveStartItem()).map(tableModel::convert).orElse(null))
                        .withHashKeyValues(model);
        final QueryResultPage<M> resultPage = dynamoDbMapper.queryPage(modelClass, expression, mapperConfig);
        return PageResult.<M>builder()
                .items(resultPage.getResults())
                .lastEvaluatedItem(Optional.ofNullable(resultPage.getLastEvaluatedKey()).map(tableModel::unconvert).orElse(null))
                .build();
    }
}
