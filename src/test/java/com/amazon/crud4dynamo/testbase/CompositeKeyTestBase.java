package com.amazon.crud4dynamo.testbase;

import com.amazon.crud4dynamo.crudinterface.CompositeKeyCrud;
import com.amazon.crud4dynamo.extension.FailedBatch;
import com.amazon.crud4dynamo.extension.PageRequest;
import com.amazon.crud4dynamo.utility.PageResultCollector;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class CompositeKeyTestBase<M, D extends CompositeKeyCrud> extends SingleTableDynamoDbTestBase<M> {
    @Getter
    private D dao;

    protected abstract D newDao();

    /** Cannot be empty */
    protected abstract List<M> getTestData();

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        dao = newDao();
        Preconditions.checkArgument(getTestData().size() > 0, "Test data cannot be empty");
    }

    @Test
    public void save() {
        final M item = getTestData().get(0);
        assertThat(getItem(item)).isEmpty();

        dao.save(item);

        assertThat(getItem(item)).contains(item);
    }

    @Test
    public void saveAll() {
        assertTrue(dao.saveAll(getTestData()).isEmpty());

        assertThat(getAllItems()).containsAll(getTestData());
    }

    @Test
    public void saveAll_failed() {
        shutdownDb();

        final FailedBatch failedBatch = dao.saveAll(getTestData());

        assertThat(failedBatch.getFailedItems()).containsOnlyElementsOf(getTestData());
    }

    @Test
    public void delete() {
        save();
        final M model = getTestData().get(0);

        dao.delete(model);

        assertThat(getItem(model)).isEmpty();
    }

    @Test
    public void findAll() {
        saveAll();

        final List result = Lists.newArrayList(dao.findAll());

        assertThat(result).containsOnlyElementsOf(getTestData());
    }

    @Test
    public void deleteBy() {
        save();
        final M model = getTestData().get(0);

        dao.deleteBy(getDynamoDbMapperTableModel().hashKey().get(model), getDynamoDbMapperTableModel().rangeKey().get(model));

        assertThat(getItem(model)).isEmpty();
    }

    @Test
    public void deleteAll() {
        saveAll();

        final FailedBatch result = dao.deleteAll(getTestData());

        assertTrue(result.isEmpty());
        assertThat(getAllItems()).isEmpty();
    }

    @Test
    public void deleteAll_failed() {
        saveAll();
        shutdownDb();

        final List result = dao.deleteAll(getTestData()).getFailedItems();

        assertThat(result).containsOnlyElementsOf(getTestData());
    }

    @Test
    public void findBy() {
        saveAll();
        final M model = getTestData().get(0);

        final Optional result =
                dao.findBy(getDynamoDbMapperTableModel().hashKey().get(model), getDynamoDbMapperTableModel().rangeKey().get(model));

        assertThat(result).contains(model);
    }

    @Test
    public void findAllBy() {
        saveAll();

        final List result = Lists.newArrayList(dao.findAll());

        assertThat(result).containsAll(getTestData());
    }

    @Test
    public void findAllWithPaging() {
        saveAll();

        final List<M> result = newPageResultCollector(req -> dao.findAll(req)).get();

        assertThat(result).containsAll(getTestData());
    }

    @Test
    public void groupBy() {
        saveAll();
        final M model = getTestData().get(0);
        final Object hashKey = getDynamoDbMapperTableModel().hashKey().get(model);

        final List result = Lists.newArrayList(dao.groupBy(hashKey));

        assertThat(result).containsAll(groupByHashKey(hashKey));
    }

    private List groupByHashKey(final Object hashKey) {
        final DynamoDBQueryExpression expression =
                new DynamoDBQueryExpression<>()
                        .withConsistentRead(false)
                        .withKeyConditionExpression(String.format("%s = :val", getDynamoDbMapperTableModel().hashKey().name()))
                        .withExpressionAttributeValues(ImmutableMap.of(":val", getDynamoDbMapperTableModel().hashKey().convert(hashKey)));
        return Lists.newArrayList(getDynamoDbMapper().query(getModelClass(), expression).iterator());
    }

    @Test
    public void groupByWithPaging() {
        saveAll();
        final M model = getTestData().get(0);
        final Object hashKey = getDynamoDbMapperTableModel().hashKey().get(model);

        final List<M> result = newPageResultCollector(req -> dao.groupBy(hashKey, req)).get();

        assertThat(result).containsAll(groupByHashKey(hashKey));
    }

    private PageResultCollector<M, M> newPageResultCollector(final PageResultCollector.Requester<M> requester) {
        return PageResultCollector.newCollector(PageRequest.<M>builder().exclusiveStartItem(null).limit(1).build(), requester);
    }

    @Test
    public void groupByPage() {
        final M model = getTestData().get(0);
        final Object hashKey = getDynamoDbMapperTableModel().hashKey().get(model);

        final List<Object> result =
                PageResultCollector.newCollector(
                                PageRequest.builder().limit(1).exclusiveStartItem(null).build(), request -> dao.groupBy(hashKey, request))
                        .get();

        assertThat(result).containsAll(Lists.newArrayList(groupByHashKey(hashKey)));
    }

    @Test
    public void invocationFailed_directlyThrowException() {
        shutdownDb();

        assertThatThrownBy(() -> dao.save(getTestData().get(0))).isInstanceOf(AmazonServiceException.class);
    }
}
