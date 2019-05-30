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
import com.amazon.crud4dynamo.extension.PageRequest;
import com.amazon.crud4dynamo.extension.PageResult;
import java.util.Iterator;

/**
 * Base DynamoDB CRUD interface.
 *
 * <p>Don't use this interface directly, use SimpleKeyCrud or CompositeKeyCrud
 *
 * @param <M> Model Type Generic Parameter
 */
public interface DynamoDbCrud<M> {
    void save(final M model) throws CrudForDynamoException;

    FailedBatch<M> saveAll(final Iterable<M> models) throws CrudForDynamoException;

    void delete(final M model) throws CrudForDynamoException;

    FailedBatch<M> deleteAll(final Iterable<M> models) throws CrudForDynamoException;

    Iterator<M> findAll() throws CrudForDynamoException;

    PageResult<M> findAll(final PageRequest<M> pageRequest) throws CrudForDynamoException;
}
