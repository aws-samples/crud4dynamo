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

import com.amazon.crud4dynamo.extension.PageRequest;
import com.amazon.crud4dynamo.internal.factory.ExpressionFactoryHelper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.google.common.base.Preconditions;
import java.util.Optional;
import lombok.NonNull;

public class PagingExpressionFactory implements ScanExpressionFactory {

  private final ScanExpressionFactory expressionFactory;
  private final Class<?> tableType;
  private final DynamoDBMapper mapper;

  PagingExpressionFactory(
      @NonNull final ScanExpressionFactory expressionFactory,
      @NonNull final Class<?> tableType,
      @NonNull final DynamoDBMapper mapper) {
    this.expressionFactory = expressionFactory;
    this.tableType = tableType;
    this.mapper = mapper;
  }

  @Override
  public DynamoDBScanExpression create(final Object... args) {
    final DynamoDBScanExpression expression = expressionFactory.create(args);
    final Optional<PageRequest> pageRequest = ExpressionFactoryHelper.findPageRequest(args);
    Preconditions.checkArgument(pageRequest.isPresent(), "No page request found.");
    return expression
        .withLimit(pageRequest.get().getLimit())
        .withExclusiveStartKey(
            ExpressionFactoryHelper.getLastEvaluatedKey(
                pageRequest.get(), mapper.getTableModel(tableType)));
  }
}
