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
package com.amazon.crud4dynamo;

import com.amazon.crud4dynamo.crudinterface.CompositeKeyCrud;
import com.amazon.crud4dynamo.crudinterface.DynamoDbCrud;
import com.amazon.crud4dynamo.crudinterface.SimpleKeyCrud;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.factory.ChainedMethodFactoryConfig;
import com.amazon.crud4dynamo.internal.Proxy;
import com.amazon.crud4dynamo.internal.config.DefaultCrudFactoryConfig;
import com.amazon.crud4dynamo.internal.config.DefaultTransactionFactoryConfig;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.google.common.base.Suppliers;
import com.google.common.reflect.TypeToken;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CrudForDynamo {
  private static final Config DEFAULT_CONFIG =
      Config.builder()
          .mapperConfig(DynamoDBMapperConfig.DEFAULT)
          .crudFactoryConstructorConfigs(DefaultCrudFactoryConfig.getConfigs())
          .transactionFactoryConstructorConfigs(DefaultTransactionFactoryConfig.getConfigs())
          .build();
  private final Supplier<AbstractMethodFactory> crudMethodFactorySupplier;
  private final Supplier<AbstractMethodFactory> transactionMethodFactorySupplier;
  private final Supplier<DynamoDBMapper> dynamoDbMapperSupplier;
  private final AmazonDynamoDB dynamoDb;

  private final Config mergedConfig;

  public CrudForDynamo(final AmazonDynamoDB dynamoDb) {
    this(dynamoDb, null);
  }

  public CrudForDynamo(final AmazonDynamoDB dynamoDb, final Config config) {
    this.dynamoDb = dynamoDb;
    this.mergedConfig = Config.merge(DEFAULT_CONFIG, config);
    this.crudMethodFactorySupplier =
        Suppliers.memoize(() -> newChainedFactories(mergedConfig.crudFactoryConstructorConfigs()));
    this.transactionMethodFactorySupplier =
        Suppliers.memoize(
            () -> newChainedFactories(mergedConfig.transactionFactoryConstructorConfigs()));
    this.dynamoDbMapperSupplier = Suppliers.memoize(() -> new DynamoDBMapper(dynamoDb));
  }

  private static Class<?> getModelType(final Class<?> interfaceType) {
    final TypeToken<?> typeToken = TypeToken.of(interfaceType);
    if (typeToken.isSubtypeOf(SimpleKeyCrud.class)) {
      return typeToken.resolveType(SimpleKeyCrud.class.getTypeParameters()[1]).getRawType();
    } else {
      return typeToken.resolveType(CompositeKeyCrud.class.getTypeParameters()[2]).getRawType();
    }
  }

  private static AbstractMethodFactory newChainedFactories(
      final List<ChainedMethodFactoryConfig> configs) {
    String chainedString = "null";
    AbstractMethodFactory chained = null;
    for (final ChainedMethodFactoryConfig config : reverse(configs)) {
      chained = config.getChainedFactoryConstructor().apply(chained);
      chainedString = chained.getClass().getSimpleName() + "(" + chainedString + ")";
    }
    log.info("Constructed chained method factory: {}", chainedString);
    return chained;
  }

  private static List<ChainedMethodFactoryConfig> reverse(
      final List<ChainedMethodFactoryConfig> configs) {
    final List<ChainedMethodFactoryConfig> reversed = new ArrayList<>(configs);
    Collections.reverse(reversed);
    return reversed;
  }

  public <T> T createTransaction(final Class<T> transactionInterface) {
    log.info("Create transactions for interface '{}'.", transactionInterface.getSimpleName());
    final AbstractMethodFactory factory = transactionMethodFactorySupplier.get();
    return new Proxy<>(
            transactionInterface,
            method -> factory.create(newContext(transactionInterface, null, method)))
        .create();
  }

  @SuppressWarnings("unchecked")
  public <K, M> SimpleKeyCrud<K, M> createSimple(final Class<M> modelClass) {
    return create(SimpleKeyCrud.class, modelClass);
  }

  @SuppressWarnings("unchecked")
  public <H, R, M> CompositeKeyCrud<H, R, M> createComposite(final Class<M> modelClass) {
    return create(CompositeKeyCrud.class, modelClass);
  }

  public <T extends DynamoDbCrud> T create(final Class<T> interfaceType) {
    final Class<?> modelClass = getModelType(interfaceType);
    return create(interfaceType, modelClass);
  }

  private <T extends DynamoDbCrud> T create(
      final Class<T> interfaceType, final Class<?> modelClass) {
    log.info("Create proxy for interface '{}' with modelClass '{}'.", interfaceType, modelClass);
    final AbstractMethodFactory factory = crudMethodFactorySupplier.get();
    return new Proxy<>(
            interfaceType, method -> factory.create(newContext(interfaceType, modelClass, method)))
        .create();
  }

  private Context newContext(
      final Class<?> interfaceType, final Class<?> modelClass, final Method method) {
    return Context.builder()
        .amazonDynamoDb(dynamoDb)
        .mapper(dynamoDbMapperSupplier.get())
        .mapperConfig(mergedConfig.mapperConfig())
        .modelType(modelClass)
        .interfaceType(interfaceType)
        .signature(Signature.resolve(method, interfaceType))
        .method(method)
        .build();
  }
}
