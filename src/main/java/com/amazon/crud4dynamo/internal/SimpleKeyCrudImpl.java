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

import com.amazon.crud4dynamo.crudinterface.SimpleKeyCrud;
import com.amazon.crud4dynamo.extension.FailedBatch;
import com.amazon.crud4dynamo.utility.Reflection;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperFieldModel;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperTableModel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This class provides implementation only for operations declared in SimpleKeyCrud interface.
 *
 * @param <H> HashKey Generic Type Parameter
 * @param <M> Model Generic Type Parameter
 */
public class SimpleKeyCrudImpl<H, M> extends DynamoDbCrudBase<M> implements SimpleKeyCrud<H, M> {

  public SimpleKeyCrudImpl(
      final DynamoDBMapper dynamoDbMapper,
      final DynamoDBMapperConfig mapperConfig,
      final Class<M> modelClass) {
    super(dynamoDbMapper, mapperConfig, modelClass);
  }

  @Override
  public void deleteBy(final H hashKey) {
    final M model = Reflection.newInstance(modelClass);
    dynamoDbMapper.getTableModel(modelClass).hashKey().set(model, hashKey);
    dynamoDbMapper.delete(model, mapperConfig);
  }

  @Override
  public FailedBatch<H> deleteAllBy(final Iterable<H> hashKeys) {
    final List<M> models = convertHashKeysToModels(hashKeys);
    final FailedBatch<M> modelFailedBatch = deleteAll(models);
    return FailedBatch.<H>builder()
        .subBatches(convertToKeySubBatches(tableModel, modelFailedBatch))
        .build();
  }

  private List<FailedBatch.SubBatch<H>> convertToKeySubBatches(
      final DynamoDBMapperTableModel<M> tableModel, final FailedBatch<M> modelFailedBatch) {
    return modelFailedBatch.getSubBatches().stream()
        .map(modelSubBatch -> buildHashKeySubBatch(modelSubBatch, tableModel))
        .collect(Collectors.toList());
  }

  private FailedBatch.SubBatch<H> buildHashKeySubBatch(
      final FailedBatch.SubBatch<M> modelSubBatch, final DynamoDBMapperTableModel<M> tableModel) {
    return FailedBatch.SubBatch.<H>builder()
        .failedItems(convertToHashKeys(modelSubBatch, tableModel))
        .exception(modelSubBatch.getException())
        .build();
  }

  private List<H> convertToHashKeys(
      final FailedBatch.SubBatch<M> modelSubBatch, final DynamoDBMapperTableModel<M> tableModel) {
    return modelSubBatch.getFailedItems().stream()
        .map(model -> tableModel.<H>hashKey().get(model))
        .collect(Collectors.toList());
  }

  @Override
  public Optional<M> findBy(final H hashKey) {
    return Optional.ofNullable(dynamoDbMapper.load(modelClass, hashKey, mapperConfig));
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterator<M> findAllBy(final Iterable<H> hashKeys) {
    final List<M> models = convertHashKeysToModels(hashKeys);
    return (Iterator<M>)
        dynamoDbMapper.batchLoad(models, mapperConfig).values().stream()
            .flatMap(Collection::stream)
            .iterator();
  }

  private List<M> convertHashKeysToModels(final Iterable<H> hashKeys) {
    final DynamoDBMapperFieldModel<M, H> hashKeyField = tableModel.hashKey();
    final List<M> models = new ArrayList<>();
    for (final H hashKey : hashKeys) {
      final M model = Reflection.newInstance(modelClass);
      hashKeyField.set(model, hashKey);
      models.add(model);
    }
    return models;
  }
}
