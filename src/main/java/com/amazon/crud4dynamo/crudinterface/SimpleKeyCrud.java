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
import com.amazon.crud4dynamo.extension.FailedBatch;
import java.util.Iterator;
import java.util.Optional;

/**
 * CRUD Interface for model with simple primary key.
 *
 * @param <H> HashKey Type Generic Parameter
 * @param <M> Model Type Generic Parameter
 */
public interface SimpleKeyCrud<H, M> extends DynamoDbCrud<M> {
  void deleteBy(final H hashKey) throws CrudForDynamoException;

  FailedBatch<H> deleteAllBy(final Iterable<H> hashKeys) throws CrudForDynamoException;

  Optional<M> findBy(final H hashKey) throws CrudForDynamoException;

  Iterator<M> findAllBy(final Iterable<H> hashKeys) throws CrudForDynamoException;
}
