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
