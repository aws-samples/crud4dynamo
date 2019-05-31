package com.amazon.crud4dynamo.internal.method.transaction;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.annotation.transaction.ConditionCheck;
import com.amazon.crud4dynamo.extension.Argument;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ReturnValuesOnConditionCheckFailure;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;

class ConditionCheckFactoryTest
    extends SingleTableDynamoDbTestBase<ConditionCheckFactoryTest.Table> {
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @DynamoDBTable(tableName = Table.NAME)
  public static class Table {
    private static final String NAME = "TestTable";
    private static final String HASH_KEY = "HashKey";
    private static final String STRING_ATTRIBUTE = "StringAttribute";

    @DynamoDBHashKey(attributeName = HASH_KEY)
    private String hashKey;

    @DynamoDBAttribute(attributeName = STRING_ATTRIBUTE)
    private String stringAttribute;
  }

  @Override
  protected Class<Table> getModelClass() {
    return Table.class;
  }

  private interface Dao {
    String CONDITION_EXPRESSION_1 = "attribute_exists(#attribute)";
    String CONDITION_EXPRESSION_2 = "StringAttribute > :value";

    @ConditionCheck(
        tableClass = Table.class,
        keyExpression = "#key = :hashKey",
        conditionExpression = CONDITION_EXPRESSION_1)
    void checkWithoutExpressionAttributeValues(
        @Param("#key") final String keyName,
        @Param(":hashKey") final String hashKey,
        @Param("#attribute") final String attribute);

    @ConditionCheck(
        tableClass = Table.class,
        keyExpression = "HashKey = :hashKey",
        conditionExpression = CONDITION_EXPRESSION_2)
    void checkWithExpressionAttributeValues(
        @Param(":hashKey") final String hashKey, @Param(":value") final String value);
  }

  /* TODO: clean up. */
  @Test
  void checkWithoutExpressionAttributeValues() throws Exception {
    final Method method =
        Dao.class.getMethod(
            "checkWithoutExpressionAttributeValues", String.class, String.class, String.class);
    final Signature signature = Signature.resolve(method, Dao.class);
    final ConditionCheck annotation = signature.getAnnotation(ConditionCheck.class).get();
    final ConditionCheckFactory conditionCheckFactory =
        new ConditionCheckFactory(annotation, getDynamoDbMapperTableModel());
    final String aHashKey = "aHashKey";
    final List<Argument> arguments =
        Argument.newList(
            signature.parameters(),
            Arrays.asList(Table.HASH_KEY, aHashKey, Table.STRING_ATTRIBUTE));

    final com.amazonaws.services.dynamodbv2.model.ConditionCheck conditionCheck =
        conditionCheckFactory.create(arguments);

    assertThat(conditionCheck.getTableName()).isEqualTo(Table.NAME);
    assertThat(conditionCheck.getConditionExpression()).isEqualTo(Dao.CONDITION_EXPRESSION_1);
    assertThat(conditionCheck.getExpressionAttributeNames())
        .containsEntry("#attribute", Table.STRING_ATTRIBUTE);
    assertThat(conditionCheck.getExpressionAttributeValues()).isNull();
    assertThat(conditionCheck.getReturnValuesOnConditionCheckFailure())
        .isEqualTo(ReturnValuesOnConditionCheckFailure.NONE.toString());
  }

  @Test
  void checkWithExpressionAttributeValues() throws Exception {
    final Method method =
        Dao.class.getMethod("checkWithExpressionAttributeValues", String.class, String.class);
    final Signature signature = Signature.resolve(method, Dao.class);
    final ConditionCheck annotation = signature.getAnnotation(ConditionCheck.class).get();
    final ConditionCheckFactory conditionCheckFactory =
        new ConditionCheckFactory(annotation, getDynamoDbMapperTableModel());
    final String aHashKey = "aHashKey";
    final String stringValue = "dummy";
    final List<Argument> arguments =
        Argument.newList(signature.parameters(), Arrays.asList(aHashKey, stringValue));

    final com.amazonaws.services.dynamodbv2.model.ConditionCheck conditionCheck =
        conditionCheckFactory.create(arguments);

    assertThat(conditionCheck.getTableName()).isEqualTo(Table.NAME);
    assertThat(conditionCheck.getConditionExpression()).isEqualTo(Dao.CONDITION_EXPRESSION_2);
    assertThat(conditionCheck.getExpressionAttributeNames()).isNull();
    assertThat(conditionCheck.getExpressionAttributeValues())
        .containsEntry(":value", new AttributeValue(stringValue));
    assertThat(conditionCheck.getReturnValuesOnConditionCheckFailure())
        .isEqualTo(ReturnValuesOnConditionCheckFailure.NONE.toString());
  }
}
