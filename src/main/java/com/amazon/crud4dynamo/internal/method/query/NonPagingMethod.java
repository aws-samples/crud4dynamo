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

import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import lombok.NonNull;

public class NonPagingMethod implements AbstractMethod {
  private final Signature signature;
  private final Class<?> tableType;
  private final DynamoDBMapper mapper;
  private final QueryExpressionFactory expressionFactory;
  private final DynamoDBMapperConfig mapperConfig;

  public NonPagingMethod(
      @NonNull final Signature signature,
      @NonNull final Class<?> tableType,
      @NonNull final DynamoDBMapper mapper,
      final DynamoDBMapperConfig mapperConfig) {
    this.signature = signature;
    this.tableType = tableType;
    this.mapper = mapper;
    this.mapperConfig = mapperConfig;
    expressionFactory = new NonPagingExpressionFactory(signature, tableType, mapper);
  }

  @Override
  public AbstractMethod bind(final Object target) {
    return this;
  }

  @Override
  public Signature getSignature() {
    return signature;
  }

  @Override
  public Object invoke(final Object... args) throws Throwable {
    return mapper.query(tableType, expressionFactory.create(args), mapperConfig).iterator();
  }
}
