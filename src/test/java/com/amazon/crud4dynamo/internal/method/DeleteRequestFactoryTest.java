package com.amazon.crud4dynamo.internal.method;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.amazon.crud4dynamo.annotation.Delete;
import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Method;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;

class DeleteRequestFactoryTest extends SingleTableDynamoDbTestBase<DeleteRequestFactoryTest.Model> {
  private static final String TABLE_NAME = "Table";
  private static final String HASH_KEY_NAME = "HashKey";
  private static final String RANGE_KEY_NAME = "RangeKey";
  private static final String STR_ATTRIBUTE_NAME = "Str1";
  private static final String SIMPLE_KEY_EXPRESSION = "HashKey = :hashKey, RangeKey = :rangeKey";
  private static final String COMPLEX_KEY_EXPRESSION = "#hashKey = :hashKey, #rangeKey = :rangeKey";
  private static final String CONDITIONAL_EXPRESSION_1 = "#nonKeyAttribute <> :value";
  private static final String CONDITIONAL_EXPRESSION_2 = "#hashKey <> :hashKey";

  @Override
  protected Class<Model> getModelClass() {
    return Model.class;
  }

  @Test
  void methodWithoutDeleteAnnotation_throwException() throws Exception {
    assertThatThrownBy(() -> newFactory(Dao.class.getMethod("methodWithoutDeleteAnnotation")))
        .isInstanceOf(DeleteRequestFactory.NoDeleteAnnotationException.class);
  }

  @Test
  void method_withNotNonReturnValue_and_nonModelReturnType_throwException() {
    assertThatThrownBy(() -> newFactory(Dao.class.getMethod("methodWithNotNonReturnValue")))
        .isInstanceOf(DeleteRequestFactory.ReturnTypeInvalidException.class);
  }

  @Test
  void delete_withSimpleKeyExpression() throws Exception {
    final DeleteRequestFactory factory =
        newFactory(Dao.class.getMethod("delete", String.class, Integer.class));
    final String dummyHashKeyValue = "dummy1";
    final Integer dummyRangeKeyValue = 1;

    final DeleteItemRequest deleteItemRequest =
        factory.create(dummyHashKeyValue, dummyRangeKeyValue);

    assertThat(deleteItemRequest)
        .isEqualTo(
            new DeleteItemRequest()
                .withTableName(TABLE_NAME)
                .withKey(
                    getDynamoDbMapperTableModel().convertKey(dummyHashKeyValue, dummyRangeKeyValue))
                .withReturnValues(ReturnValue.NONE));
  }

  @Test
  void delete_withComplexKeyExpression() throws Exception {
    final DeleteRequestFactory factory =
        newFactory(
            Dao.class.getMethod("delete", String.class, String.class, String.class, Integer.class));
    final String dummyHashKeyValue = "dummy1";
    final Integer dummyRangeKeyValue = 1;

    final DeleteItemRequest deleteItemRequest =
        factory.create(HASH_KEY_NAME, RANGE_KEY_NAME, dummyHashKeyValue, dummyRangeKeyValue);

    assertThat(deleteItemRequest)
        .isEqualTo(
            new DeleteItemRequest()
                .withTableName(TABLE_NAME)
                .withKey(
                    getDynamoDbMapperTableModel().convertKey(dummyHashKeyValue, dummyRangeKeyValue))
                .withExpressionAttributeNames(null)
                .withReturnValues(ReturnValue.NONE));
  }

  @Test
  void conditionalDelete() throws Exception {
    final DeleteRequestFactory factory =
        newFactory(
            Dao.class.getMethod(
                "conditionalDelete", String.class, Integer.class, String.class, String.class));

    final String dummyHashKeyValue = "dummy1";
    final Integer dummyRangeKeyValue = 1;
    final String dummyStringValue = "dummy";

    final DeleteItemRequest deleteItemRequest =
        factory.create(dummyHashKeyValue, dummyRangeKeyValue, STR_ATTRIBUTE_NAME, dummyStringValue);

    assertThat(deleteItemRequest)
        .isEqualTo(
            new DeleteItemRequest()
                .withTableName(TABLE_NAME)
                .withKey(
                    getDynamoDbMapperTableModel().convertKey(dummyHashKeyValue, dummyRangeKeyValue))
                .withConditionExpression(CONDITIONAL_EXPRESSION_1)
                .withExpressionAttributeNames(
                    ImmutableMap.of("#nonKeyAttribute", STR_ATTRIBUTE_NAME))
                .withExpressionAttributeValues(
                    ImmutableMap.of(":value", new AttributeValue(dummyStringValue)))
                .withReturnValues(ReturnValue.ALL_OLD));
  }

  @Test
  void conditionDelete_keyExpression_shares_same_expressionAttributeName_with_conditionExpression()
      throws Exception {
    final DeleteRequestFactory factory =
        newFactory(
            Dao.class.getMethod(
                "conditionalDelete", String.class, String.class, String.class, Integer.class));
    final String dummyHashKeyValue = "dummy1";
    final Integer dummyRangeKeyValue = 1;

    final DeleteItemRequest deleteItemRequest =
        factory.create(HASH_KEY_NAME, RANGE_KEY_NAME, dummyHashKeyValue, dummyRangeKeyValue);

    assertThat(deleteItemRequest)
        .isEqualTo(
            new DeleteItemRequest()
                .withTableName(TABLE_NAME)
                .withKey(
                    getDynamoDbMapperTableModel().convertKey(dummyHashKeyValue, dummyRangeKeyValue))
                .withConditionExpression(CONDITIONAL_EXPRESSION_2)
                .withExpressionAttributeNames(ImmutableMap.of("#hashKey", HASH_KEY_NAME))
                .withExpressionAttributeValues(
                    ImmutableMap.of(":hashKey", new AttributeValue(dummyHashKeyValue)))
                .withReturnValues(ReturnValue.NONE));
  }

  private DeleteRequestFactory newFactory(final Method method) {
    return new DeleteRequestFactory(
        Signature.resolve(method, Dao.class), getModelClass(), getDynamoDbMapper());
  }

  private interface Dao {
    void methodWithoutDeleteAnnotation();

    @Delete(returnValue = ReturnValue.ALL_OLD, keyExpression = SIMPLE_KEY_EXPRESSION)
    void methodWithNotNonReturnValue();

    @Delete(keyExpression = SIMPLE_KEY_EXPRESSION)
    void delete(
        final @Param(":hashKey") String hashKeyValue,
        final @Param(":rangeKey") Integer rangeKeyValue);

    @Delete(keyExpression = COMPLEX_KEY_EXPRESSION)
    void delete(
        final @Param("#hashKey") String hashKeyName,
        final @Param("#rangeKey") String rangeKeyName,
        final @Param(":hashKey") String hashKeyValue,
        final @Param(":rangeKey") Integer rangeKeyValue);

    @Delete(
        returnValue = ReturnValue.ALL_OLD,
        keyExpression = SIMPLE_KEY_EXPRESSION,
        conditionExpression = CONDITIONAL_EXPRESSION_1)
    Model conditionalDelete(
        final @Param(":hashKey") String hashKeyValue,
        final @Param(":rangeKey") Integer rangeKeyValue,
        final @Param("#nonKeyAttribute") String nonKeyAttribute,
        final @Param(":value") String value);

    @Delete(keyExpression = COMPLEX_KEY_EXPRESSION, conditionExpression = CONDITIONAL_EXPRESSION_2)
    void conditionalDelete(
        final @Param("#hashKey") String hashKeyName,
        final @Param("#rangeKey") String rangeKeyName,
        final @Param(":hashKey") String hashKeyValue,
        final @Param(":rangeKey") Integer rangeKeyValue);
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @DynamoDBTable(tableName = TABLE_NAME)
  public static class Model {
    @DynamoDBHashKey(attributeName = HASH_KEY_NAME)
    private String hashKey;

    @DynamoDBRangeKey(attributeName = RANGE_KEY_NAME)
    private Integer rangeKey;

    @DynamoDBAttribute(attributeName = STR_ATTRIBUTE_NAME)
    private String str1;
  }
}
