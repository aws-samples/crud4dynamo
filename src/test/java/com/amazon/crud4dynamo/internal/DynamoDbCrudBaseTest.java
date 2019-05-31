package com.amazon.crud4dynamo.internal;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazon.crud4dynamo.extension.FailedBatch;
import com.amazon.crud4dynamo.extension.PageRequest;
import com.amazon.crud4dynamo.internal.DynamoDbCrudBaseTest.Model;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazon.crud4dynamo.utility.PageResultCollector;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DynamoDbCrudBaseTest extends SingleTableDynamoDbTestBase<Model> {
  private static final List<Model> TEST_ITEMS =
      Arrays.asList(Model.builder().hashKey("A").build(), Model.builder().hashKey("B").build());

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @DynamoDBTable(tableName = "TestTable")
  public static class Model {
    @DynamoDBHashKey(attributeName = "HashKey")
    private String hashKey;

    @DynamoDBAttribute(attributeName = "Integer1")
    private Integer integer1;
  }

  private DynamoDbCrudBase<Model> dao;

  @Override
  protected Class<Model> getModelClass() {
    return Model.class;
  }

  @Override
  @BeforeEach
  public void setUp() throws Exception {
    super.setUp();
    dao =
        new DynamoDbCrudBase<Model>(
            getDynamoDbMapper(), DynamoDBMapperConfig.DEFAULT, Model.class) {};
  }

  @Test
  void save_succeeded() {
    final Model item = TEST_ITEMS.get(0);
    assertThat(getItem(item)).isEmpty();

    dao.save(item);

    assertThat(getItem(item)).contains(item);
  }

  @Test
  void saveAll_succeeded() {
    final FailedBatch<Model> failModels = dao.saveAll(TEST_ITEMS);

    assertThat(failModels.isEmpty()).isTrue();
    assertThat(getAllItems()).containsOnlyElementsOf(TEST_ITEMS);
  }

  @Test
  void saveAll_failed() {
    shutdownDb();

    final FailedBatch<Model> failedBatch = dao.saveAll(TEST_ITEMS);

    assertThat(failedBatch.getFailedItems()).containsOnlyElementsOf(TEST_ITEMS);
  }

  @Test
  void delete_succeeded() {
    save_succeeded();
    final Model item = TEST_ITEMS.get(0);

    dao.delete(item);

    assertThat(getItem(item)).isEmpty();
  }

  @Test
  void deleteAll_failed() {
    saveAll_succeeded();
    shutdownDb();

    final FailedBatch<Model> failedBatch = dao.deleteAll(TEST_ITEMS);

    assertThat(failedBatch.getFailedItems()).containsOnlyElementsOf(TEST_ITEMS);
  }

  @Test
  void findAll_succeeded() {
    saveAll_succeeded();

    final List<Model> models = Lists.newArrayList(dao.findAll());

    assertThat(models).containsOnlyElementsOf(TEST_ITEMS);
  }

  @Test
  void findAll_pageRequest() {
    saveAll_succeeded();

    assertThat(newPageResultCollector().get()).containsAll(TEST_ITEMS);
  }

  private PageResultCollector<Model, Model> newPageResultCollector() {
    return PageResultCollector.newCollector(
        PageRequest.<Model>builder().exclusiveStartItem(null).limit(1).build(),
        req -> dao.findAll(req));
  }
}
