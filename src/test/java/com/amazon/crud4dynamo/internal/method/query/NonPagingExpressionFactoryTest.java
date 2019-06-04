package com.amazon.crud4dynamo.internal.method.query;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.annotation.Query;
import com.amazon.crud4dynamo.crudinterface.CompositeKeyCrud;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Method;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;

class NonPagingExpressionFactoryTest
    extends SingleTableDynamoDbTestBase<NonPagingExpressionFactoryTest.Model> {
  private static final String KEY_CONDITION_EXPRESSION =
      "#hashKey = :hashKey and #rangeKey between :lower and :upper";
  private static final String FILTER_EXPRESSION = "begins_with(Str1, :prefix)";

  private static ImmutableMap<String, AttributeValue> getExpectedValueMap() {
    return ImmutableMap.of(
        ":lower", new AttributeValue().withN("1"),
        ":upper", new AttributeValue().withN("10"),
        ":prefix", new AttributeValue().withS("Abc"));
  }

  private static ImmutableMap<String, String> getExpectedNameMap() {
    return ImmutableMap.of("#hashKey", "HashKey", "#rangeKey", "RangeKey");
  }

  @Override
  protected Class<Model> getModelClass() {
    return Model.class;
  }

  @Test
  void create() throws Throwable {
    final Method query =
        Dao.class.getMethod("query", String.class, int.class, int.class, int.class, String.class);
    final NonPagingExpressionFactory expressionFactory =
        new NonPagingExpressionFactory(
            Signature.resolve(query, Dao.class), getModelClass(), getDynamoDbMapper());

    final DynamoDBQueryExpression expression =
        expressionFactory.create("HashKey", "RangeKey", 1, 10, "Abc");

    assertThat(expression.isScanIndexForward()).isTrue();
    assertThat(expression.isConsistentRead()).isFalse();
    assertThat(expression.getIndexName()).isNull();
    assertThat(expression.getKeyConditionExpression()).isEqualTo(KEY_CONDITION_EXPRESSION);
    assertThat(expression.getFilterExpression()).isEqualTo(FILTER_EXPRESSION);
    assertThat(expression.getExpressionAttributeNames()).isEqualTo(getExpectedNameMap());
    assertThat(expression.getExpressionAttributeValues()).isEqualTo(getExpectedValueMap());
  }

  private interface Dao extends CompositeKeyCrud<String, Integer, Model> {
    @Query(keyCondition = KEY_CONDITION_EXPRESSION, filter = FILTER_EXPRESSION)
    Iterable<Model> query(
        @Param("#hashKey") String hashKey,
        @Param("#rangeKey") int rangeKey,
        @Param(":lower") int lower,
        @Param(":upper") int upper,
        @Param(":prefix") String prefix);
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

    @DynamoDBAttribute(attributeName = "Str1")
    private String str1;
  }
}
