package com.amazon.crud4dynamo.internal.method.scan;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.annotation.Scan;
import com.amazon.crud4dynamo.crudinterface.CompositeKeyCrud;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.internal.method.scan.NonPagingExpressionFactoryTest.Model;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import java.lang.reflect.Method;
import java.util.Iterator;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;

class NonPagingExpressionFactoryTest extends SingleTableDynamoDbTestBase<Model> {
  private static final String FILTER_EXPRESSION = "#rangeKey > :lower";

  @Override
  protected Class<Model> getModelClass() {
    return Model.class;
  }

  @Test
  void create() throws Exception {
    final NonPagingExpressionFactory factory = newFactory();

    final DynamoDBScanExpression expression = factory.create("RangeKey", 3);

    assertThat(expression.getIndexName()).isNull();
    assertThat(expression.getFilterExpression()).isEqualTo(FILTER_EXPRESSION);
    assertThat(expression.getExpressionAttributeNames()).containsEntry("#rangeKey", "RangeKey");
    assertThat(expression.getExpressionAttributeValues())
        .containsEntry(":lower", new AttributeValue().withN("3"));
  }

  private NonPagingExpressionFactory newFactory() throws NoSuchMethodException {
    final Method method = Dao.class.getMethod("scan", String.class, Integer.class);
    return new NonPagingExpressionFactory(
        Signature.resolve(method, Dao.class), getModelClass(), getDynamoDbMapper());
  }

  private interface Dao extends CompositeKeyCrud<String, Integer, Model> {
    @Scan(filter = FILTER_EXPRESSION)
    Iterator<Model> scan(@Param("#rangeKey") String rangeKey, @Param(":lower") Integer lower);
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @DynamoDBTable(tableName = "Table")
  public static class Model {
    @DynamoDBHashKey(attributeName = "HashKey")
    private String hashKey;

    @DynamoDBRangeKey(attributeName = "RangeKey")
    private Integer rangeKey;
  }
}
