package com.amazon.crud4dynamo.internal.method.query;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.annotation.Query;
import com.amazon.crud4dynamo.crudinterface.CompositeKeyCrud;
import com.amazon.crud4dynamo.extension.PageRequest;
import com.amazon.crud4dynamo.extension.PageResult;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazon.crud4dynamo.utility.PageResultCollector;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;

class PagingMethodTest extends SingleTableDynamoDbTestBase<PagingMethodTest.Model> {
  private static final String GROUP_KEY = "A";

  private static PageRequest<Model> getInitialRequest() {
    return PageRequest.<Model>builder().exclusiveStartItem(null).limit(3).build();
  }

  private static PageResultCollector.Requester getRequester(final PagingMethod queryMethod) {
    return req -> {
      try {
        return (PageResult<Model>) queryMethod.invoke(GROUP_KEY, req);
      } catch (final Throwable e) {
        throw new RuntimeException(e);
      }
    };
  }

  @Override
  protected Class<Model> getModelClass() {
    return Model.class;
  }

  @Test
  void invoke() throws Throwable {
    final List<Model> testModels = prepareTestModels();
    final PagingMethod queryMethod = getQueryMethod();

    final List<Model> models =
        PageResultCollector.newCollector(getInitialRequest(), getRequester(queryMethod)).get();

    assertThat(models).containsAll(testModels);
  }

  private PagingMethod getQueryMethod() throws NoSuchMethodException {
    final Method method = Dao.class.getMethod("query", String.class, PageRequest.class);
    return new PagingMethod(
        Signature.resolve(method, Dao.class), getModelClass(), getDynamoDbMapper(), null);
  }

  private List<Model> prepareTestModels() {
    return storeItems(
        IntStream.range(0, 10)
            .mapToObj(i -> Model.builder().hashKey(GROUP_KEY).rangeKey(i).build()));
  }

  private interface Dao extends CompositeKeyCrud<String, Integer, Model> {
    @Query(keyCondition = "HashKey = :hashKey")
    PageResult<Model> query(
        @Param(":hashKey") final String hashKey, final PageRequest<Model> request);
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @DynamoDBTable(tableName = "Model")
  public static class Model {
    @DynamoDBHashKey(attributeName = "HashKey")
    private String hashKey;

    @DynamoDBRangeKey(attributeName = "RangeKey")
    private Integer rangeKey;
  }
}
