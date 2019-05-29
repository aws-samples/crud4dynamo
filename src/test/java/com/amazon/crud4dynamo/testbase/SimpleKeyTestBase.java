package com.amazon.crud4dynamo.testbase;

import com.amazon.crud4dynamo.crudinterface.SimpleKeyCrud;
import com.amazon.crud4dynamo.extension.FailedBatch;
import com.amazon.crud4dynamo.extension.PageRequest;
import com.amazon.crud4dynamo.utility.PageResultCollector;
import com.amazonaws.AmazonServiceException;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class SimpleKeyTestBase<M, D extends SimpleKeyCrud> extends SingleTableDynamoDbTestBase<M> {

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

        assertThat(getAllItems()).containsOnlyElementsOf(getTestData());
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

        dao.deleteBy(getDynamoDbMapperTableModel().hashKey().get(model));

        assertThat(getItem(model)).isEmpty();
    }

    @Test
    public void deleteAllBy() {
        saveAll();

        final FailedBatch<String> failedBatch = dao.deleteAllBy(getHashKeys());

        assertTrue(failedBatch.getFailedItems().isEmpty());
        assertTrue(getAllItems().isEmpty());
    }

    @Test
    public void deleteAllBy_failed() {
        saveAll();
        shutdownDb();

        final FailedBatch<Object> failedBatch = dao.deleteAllBy(getHashKeys());

        assertThat(failedBatch.getFailedItems()).containsOnlyElementsOf(getHashKeys());
    }

    @Test
    public void deleteAll() {
        saveAll();

        assertTrue(dao.deleteAll(getTestData()).isEmpty());
        assertThat(getAllItems()).isEmpty();
    }

    @Test
    public void deleteAll_failed() {
        saveAll();
        shutdownDb();

        assertThat(dao.deleteAll(getTestData()).getFailedItems()).containsOnlyElementsOf(getTestData());
    }

    @Test
    public void findBy() {
        saveAll();
        final M model = getTestData().get(0);

        final Optional result = dao.findBy(getDynamoDbMapperTableModel().hashKey().get(model));

        assertThat(result).contains(model);
    }

    @Test
    public void findAllBy() {
        saveAll();

        final List result = Lists.newArrayList(dao.findAllBy(getHashKeys()));

        assertThat(result).containsOnlyElementsOf(getTestData());
    }

    @Test
    public void findAllWithPaging() {
        saveAll();

        assertThat(newPageResultCollector().get()).containsAll(getTestData());
    }

    private PageResultCollector<M, M> newPageResultCollector() {
        return PageResultCollector.newCollector(
                PageRequest.<M>builder().exclusiveStartItem(null).limit(1).build(), req -> dao.findAll(req));
    }

    @Test
    void invocationFailed_directlyThrowException() {
        shutdownDb();

        assertThatThrownBy(() -> dao.save(getTestData().get(0))).isInstanceOf(AmazonServiceException.class);
    }

    private List<Object> getHashKeys() {
        return getTestData().stream().map(data -> getDynamoDbMapperTableModel().hashKey().get(data)).collect(Collectors.toList());
    }
}
