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

    PageResult<M> groupBy(final H hashKey, final PageRequest<M> pageRequest) throws CrudForDynamoException;
}
