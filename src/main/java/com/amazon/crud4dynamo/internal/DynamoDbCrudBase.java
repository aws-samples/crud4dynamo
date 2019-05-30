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

import com.amazon.crud4dynamo.crudinterface.DynamoDbCrud;
import com.amazon.crud4dynamo.extension.FailedBatch;
import com.amazon.crud4dynamo.extension.PageRequest;
import com.amazon.crud4dynamo.extension.PageResult;
import com.amazon.crud4dynamo.internal.utility.DynamoDbMapperConfigHelper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.PaginationLoadingStrategy;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperTableModel;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.ScanResultPage;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteRequest;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class provides an implementation for basic CRUD operations.
 *
 * @param <M> Model Generic Type Parameter
 */
public abstract class DynamoDbCrudBase<M> implements DynamoDbCrud<M> {
    protected final DynamoDBMapper dynamoDbMapper;
    protected final DynamoDBMapperConfig mapperConfig;
    protected final Class<M> modelClass;
    protected final DynamoDBMapperTableModel<M> tableModel;

    public DynamoDbCrudBase(final DynamoDBMapper dynamoDbMapper, final DynamoDBMapperConfig mapperConfig, final Class<M> modelClass) {
        this.dynamoDbMapper = dynamoDbMapper;
        this.mapperConfig = mapperConfig;
        this.modelClass = modelClass;
        tableModel = dynamoDbMapper.getTableModel(modelClass);
    }

    @Override
    public void save(final M model) {
        dynamoDbMapper.save(model, mapperConfig);
    }

    @Override
    public FailedBatch<M> saveAll(final Iterable<M> models) {
        return new FailedBatchConverter<M>(dynamoDbMapper.batchWrite(models, Collections.emptyList(), mapperConfig)) {
            @Override
            protected List<M> transform(final Stream<WriteRequest> writeRequestStream) {
                return writeRequestStream
                        .map(WriteRequest::getPutRequest)
                        .map(PutRequest::getItem)
                        .map(tableModel::unconvert)
                        .collect(Collectors.toList());
            }
        }.convert();
    }

    @Override
    public void delete(final M model) {
        dynamoDbMapper.delete(model, mapperConfig);
    }

    @Override
    public FailedBatch<M> deleteAll(final Iterable<M> models) {
        return new FailedBatchConverter<M>(dynamoDbMapper.batchWrite(Collections.emptyList(), models, mapperConfig)) {
            @Override
            protected List<M> transform(final Stream<WriteRequest> writeRequestStream) {
                final List<Map<String, AttributeValue>> keyAttributeList =
                        writeRequestStream.map(WriteRequest::getDeleteRequest).map(DeleteRequest::getKey).collect(Collectors.toList());
                return filterToFindFailedModels(keyAttributeList);
            }

            private List<M> filterToFindFailedModels(final List<Map<String, AttributeValue>> keyAttributeList) {
                return Lists.newArrayList(models)
                        .stream()
                        .filter(
                                model ->
                                        keyAttributeList
                                                .stream()
                                                .anyMatch(keyAttribute -> keyAttribute.equals(tableModel.convertKey(model))))
                        .collect(Collectors.toList());
            }
        }.convert();
    }

    @Override
    public Iterator<M> findAll() {
        return dynamoDbMapper
                .scan(
                        modelClass,
                        new DynamoDBScanExpression(),
                        DynamoDbMapperConfigHelper.override(mapperConfig, PaginationLoadingStrategy.ITERATION_ONLY.config()))
                .iterator();
    }

    @Override
    public PageResult<M> findAll(final PageRequest<M> pageRequest) {
        final DynamoDBScanExpression expression =
                new DynamoDBScanExpression()
                        .withLimit(pageRequest.getLimit())
                        .withExclusiveStartKey(
                                Optional.ofNullable(pageRequest.getExclusiveStartItem()).map(tableModel::convert).orElse(null));
        final ScanResultPage<M> scanResultPage = dynamoDbMapper.scanPage(modelClass, expression, mapperConfig);
        return PageResult.<M>builder()
                .items(scanResultPage.getResults())
                .lastEvaluatedItem(Optional.ofNullable(scanResultPage.getLastEvaluatedKey()).map(tableModel::unconvert).orElse(null))
                .build();
    }

    private abstract class FailedBatchConverter<M> {
        private final List<DynamoDBMapper.FailedBatch> failedBatches;

        private FailedBatchConverter(final List<DynamoDBMapper.FailedBatch> failedBatches) {
            this.failedBatches = failedBatches;
        }

        FailedBatch<M> convert() {
            return FailedBatch.<M>builder().subBatches(buildSubBatches(failedBatches.stream())).build();
        }

        private List<FailedBatch.SubBatch<M>> buildSubBatches(final Stream<DynamoDBMapper.FailedBatch> failedBatchStream) {
            return failedBatchStream.map(this::buildSubBatch).collect(Collectors.toList());
        }

        private FailedBatch.SubBatch<M> buildSubBatch(final DynamoDBMapper.FailedBatch failedBatch) {
            final Stream<WriteRequest> writeRequestStream = failedBatch.getUnprocessedItems().values().stream().flatMap(List::stream);
            return FailedBatch.SubBatch.<M>builder()
                    .exception(failedBatch.getException())
                    .failedItems(transform(writeRequestStream))
                    .build();
        }

        protected abstract List<M> transform(final Stream<WriteRequest> writeRequestStream);
    }
}
