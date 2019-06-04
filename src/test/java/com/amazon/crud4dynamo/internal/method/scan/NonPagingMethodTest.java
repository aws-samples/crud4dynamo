package com.amazon.crud4dynamo.internal.method.scan;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.annotation.Scan;
import com.amazon.crud4dynamo.crudinterface.CompositeKeyCrud;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.google.common.collect.Lists;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;

class NonPagingMethodTest extends SingleTableDynamoDbTestBase<NonPagingMethodTest.Model> {
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
    final NonPagingMethod scanMethod = getMethod();

    final List<Model> scanResult =
        Lists.newArrayList((Iterator<Model>) scanMethod.invoke("RangeKey", 3, 7));

    assertThat(scanResult).containsAll(testData.subList(3, 8));
  }

  private NonPagingMethod getMethod() throws NoSuchMethodException {
    final Signature signature =
        Signature.resolve(
            Dao.class.getMethod("scan", String.class, int.class, int.class), Dao.class);
    return new NonPagingMethod(signature, getModelClass(), getDynamoDbMapper(), null);
  }

  public interface Dao extends CompositeKeyCrud<String, Integer, Model> {
    @Scan(filter = "#rangeKey between :lower and :upper")
    Iterator<Model> scan(
        @Param("#rangeKey") String rangeKeyName,
        @Param(":lower") int lower,
        @Param(":upper") int upper);
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
