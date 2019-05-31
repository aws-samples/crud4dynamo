package com.amazon.crud4dynamo.internal.parsing;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazon.crud4dynamo.internal.parsing.UpdateExpressionParserTest.Model;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;

class UpdateExpressionParserTest extends SingleTableDynamoDbTestBase<Model> {
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @DynamoDBTable(tableName = "TestTable")
  static class Model {
    @DynamoDBHashKey(attributeName = "HashKey")
    private String hashKey;

    @DynamoDBRangeKey(attributeName = "RangeKey")
    private Integer rangeKey;

    @DynamoDBAttribute(attributeName = "Int1")
    private int int1;

    @DynamoDBAttribute(attributeName = "Int2")
    private int int2;

    @DynamoDBAttribute(attributeName = "Int3")
    private int int3;

    @DynamoDBAttribute(attributeName = "Set1")
    private Set<String> set1;
  }

  @Data
  @Builder
  public static class CustomType {
    private String name;
    private Integer age;
  }

  @Override
  protected Class<Model> getModelClass() {
    return Model.class;
  }

  @Test
  void setExpression_case1() {
    final UpdateExpressionParser parser =
        new UpdateExpressionParser(
            "SET #attr1 = :value1, #attr2 = Int1, Int3 = :value2", getDynamoDbMapperTableModel());

    assertThat(parser.getExpressionAttributeNames()).containsOnly("#attr1", "#attr2");

    final AttributeValueMapper expectedValueMapper = new AttributeValueMapper();
    expectedValueMapper.put(
        ":value1", obj -> getDynamoDbMapperTableModel().field("Int1").convert(obj));
    expectedValueMapper.put(
        ":value2", obj -> getDynamoDbMapperTableModel().field("Int3").convert(obj));
    final AttributeValueMapper actualValueMapper =
        parser
            .getAttributeNameMapper()
            .toValueMapper(ImmutableMap.of("#attr1", "Int1"))
            .merge(parser.getAttributeValueMapper());

    verify(ImmutableMap.of(":value1", 1, ":value2", 2), actualValueMapper, expectedValueMapper);
  }

  @Test
  void setExpression_case2() {
    final UpdateExpressionParser parser =
        new UpdateExpressionParser("SET #attr1 = Int2 + :value1", getDynamoDbMapperTableModel());

    assertThat(parser.getExpressionAttributeNames()).containsOnly("#attr1");

    final AttributeValueMapper expectedValueMapper = new AttributeValueMapper();
    expectedValueMapper.put(
        ":value1", obj -> getDynamoDbMapperTableModel().field("Int1").convert(obj));

    final AttributeValueMapper actualValueMapper =
        parser
            .getAttributeNameMapper()
            .toValueMapper(ImmutableMap.of("#attr1", "Int1"))
            .merge(parser.getAttributeValueMapper());

    verify(ImmutableMap.of(":value1", 1), actualValueMapper, expectedValueMapper);
  }

  @Test
  void addExpression_case1() {
    final UpdateExpressionParser parser =
        new UpdateExpressionParser(
            "ADD #attr1 :value1, Int2 :value2", getDynamoDbMapperTableModel());

    assertThat(parser.getExpressionAttributeNames()).containsOnly("#attr1");

    final AttributeValueMapper expectedValueMapper = new AttributeValueMapper();
    expectedValueMapper.put(
        ":value1", obj -> getDynamoDbMapperTableModel().field("Int1").convert(obj));
    expectedValueMapper.put(
        ":value2", obj -> getDynamoDbMapperTableModel().field("Int2").convert(obj));

    final AttributeValueMapper actualValueMapper =
        parser
            .getAttributeNameMapper()
            .toValueMapper(ImmutableMap.of("#attr1", "Int1"))
            .merge(parser.getAttributeValueMapper());

    verify(ImmutableMap.of(":value1", 1, ":value2", 10), actualValueMapper, expectedValueMapper);
  }

  @Test
  void addExpression_case2() {
    final UpdateExpressionParser parser =
        new UpdateExpressionParser(
            "ADD Set1 :value1, #attr1 :value2", getDynamoDbMapperTableModel());
    assertThat(parser.getExpressionAttributeNames()).containsOnly("#attr1");

    final AttributeValueMapper expectedValueMapper = new AttributeValueMapper();
    expectedValueMapper.put(
        ":value1", obj -> getDynamoDbMapperTableModel().field("Set1").convert(obj));
    expectedValueMapper.put(
        ":value2", obj -> getDynamoDbMapperTableModel().field("Int1").convert(obj));

    final AttributeValueMapper actualValueMapper =
        parser
            .getAttributeNameMapper()
            .toValueMapper(ImmutableMap.of("#attr1", "Int1"))
            .merge(parser.getAttributeValueMapper());

    verify(
        ImmutableMap.of(":value1", ImmutableSet.of("A"), ":value2", ImmutableSet.of("B")),
        actualValueMapper,
        expectedValueMapper);
  }

  @Test
  void deleteExpression_case1() {
    final UpdateExpressionParser parser =
        new UpdateExpressionParser("DELETE #attr1 :value", getDynamoDbMapperTableModel());

    assertThat(parser.getExpressionAttributeNames()).containsOnly("#attr1");

    final AttributeValueMapper expectedValueMapper = new AttributeValueMapper();
    expectedValueMapper.put(
        ":value", obj -> getDynamoDbMapperTableModel().field("Set1").convert(obj));

    final AttributeValueMapper actualValueMapper =
        parser
            .getAttributeNameMapper()
            .toValueMapper(ImmutableMap.of("#attr1", "Set1"))
            .merge(parser.getAttributeValueMapper());

    verify(
        ImmutableMap.of(":value", ImmutableSet.of("hello")),
        actualValueMapper,
        expectedValueMapper);
  }

  private static void verify(
      final Map<String, Object> arguments,
      final AttributeValueMapper actualValueMapper,
      final AttributeValueMapper expectedValueMapper) {
    assertThat(applyArguments(arguments, actualValueMapper))
        .isEqualTo(applyArguments(arguments, expectedValueMapper));
  }

  private static List<AttributeValue> applyArguments(
      final Map<String, Object> arguments, final AttributeValueMapper valueMapper) {
    return arguments.keySet().stream()
        .map(key -> valueMapper.get(key).convert(arguments.get(key)))
        .collect(Collectors.toList());
  }
}
