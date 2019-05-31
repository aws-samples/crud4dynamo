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

package com.amazon.crud4dynamo.crudinterface;

import com.amazon.crud4dynamo.exception.CrudForDynamoException;
import com.amazon.crud4dynamo.extension.PageRequest;
import com.amazon.crud4dynamo.extension.PageResult;
import java.util.Iterator;
import java.util.Optional;

/**
 * CRUD Interface for model with composite primary key.
 *
 * @param <H> HashKey Generic Type Parameter
 * @param <R> RangeKey Generic Type Parameter
 * @param <M> Model Generic Type Parameter
 */
public interface CompositeKeyCrud<H, R, M> extends DynamoDbCrud<M> {
  void deleteBy(final H hashKey, final R rangeKey) throws CrudForDynamoException;

  Optional<M> findBy(final H hashKey, final R rangeKey) throws CrudForDynamoException;

  Iterator<M> groupBy(final H hashKey) throws CrudForDynamoException;

  PageResult<M> groupBy(final H hashKey, final PageRequest<M> pageRequest)
      throws CrudForDynamoException;
}
