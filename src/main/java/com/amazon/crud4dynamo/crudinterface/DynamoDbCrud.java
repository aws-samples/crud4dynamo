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
