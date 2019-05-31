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

import com.amazon.crud4dynamo.exception.CrudForDynamoException;
import com.amazon.crud4dynamo.extension.PageRequest;
import com.amazon.crud4dynamo.internal.factory.ExpressionFactoryHelper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import java.util.Optional;

public class PagingExpressionFactory implements QueryExpressionFactory {
  private final QueryExpressionFactory expressionFactory;
  private final Class<?> tableType;
  private final DynamoDBMapper mapper;

  PagingExpressionFactory(
      final QueryExpressionFactory expressionFactory,
      final Class<?> tableType,
      final DynamoDBMapper mapper) {
    this.expressionFactory = expressionFactory;
    this.tableType = tableType;
    this.mapper = mapper;
  }

  @Override
  public DynamoDBQueryExpression create(final Object... args) {
    final Optional<PageRequest> pageRequest = ExpressionFactoryHelper.findPageRequest(args);
    if (!pageRequest.isPresent()) {
      throw new CrudForDynamoException("No PageRequest argument found");
    }
    return expressionFactory
        .create(args)
        .withExclusiveStartKey(
            ExpressionFactoryHelper.getLastEvaluatedKey(
                pageRequest.get(), mapper.getTableModel(tableType)))
        .withLimit(pageRequest.get().getLimit());
  }
}
