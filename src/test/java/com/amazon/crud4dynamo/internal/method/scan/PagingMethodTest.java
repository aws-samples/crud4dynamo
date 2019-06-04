package com.amazon.crud4dynamo.internal.method.scan;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.annotation.Scan;
import com.amazon.crud4dynamo.crudinterface.CompositeKeyCrud;
import com.amazon.crud4dynamo.extension.PageRequest;
import com.amazon.crud4dynamo.extension.PageResult;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.internal.method.scan.PagingMethodTest.Model;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazon.crud4dynamo.utility.PageResultCollector;
import com.amazon.crud4dynamo.utility.PageResultCollector.Requester;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;

class PagingMethodTest extends SingleTableDynamoDbTestBase<Model> {

  private static Requester<Model> newRequester(final PagingMethod scanMethod) {
    return req -> {
      try {
        return (PageResult<Model>) scanMethod.invoke("RangeKey", 3, 9, req);
      } catch (final Throwable throwable) {
        throw new RuntimeException(throwable);
      }
    };
  }

  private static Stream<Model> prepareData() {
    return IntStream.range(0, 10).mapToObj(i -> Model.builder().hashKey("A").rangeKey(i).build());
  }

  @Override
  protected Class<Model> getModelClass() {
    return Model.class;
  }

  @Test
  void scan() throws Throwable {
    final List<Model> testData = storeItems(prepareData());
    final PagingMethod scanMethod = getMethod();

    final List<Model> models =
        PageResultCollector.newCollector(
                PageRequest.<Model>builder().limit(1).build(), newRequester(scanMethod))
            .get();

    assertThat(models).containsAll(testData.subList(3, 10));
  }

  private PagingMethod getMethod() throws NoSuchMethodException {
    final Signature signature =
        Signature.resolve(
            Dao.class.getMethod("scan", String.class, int.class, int.class, PageRequest.class),
            Dao.class);
    return new PagingMethod(signature, getModelClass(), getDynamoDbMapper(), null);
  }

  public interface Dao extends CompositeKeyCrud<String, Integer, Model> {
    @Scan(filter = "#rangeKey between :lower and :upper")
    PageResult<Model> scan(
        @Param("#rangeKey") String rangeKeyName,
        @Param(":lower") int lower,
        @Param(":upper") int upper,
        final PageRequest<Model> request);
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
