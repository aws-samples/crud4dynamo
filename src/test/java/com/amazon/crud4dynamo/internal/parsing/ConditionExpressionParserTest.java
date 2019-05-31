package com.amazon.crud4dynamo.internal.parsing;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class ConditionExpressionParserTest
    extends SingleTableDynamoDbTestBase<ConditionExpressionParserTest.Model> {
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

  @Override
  protected Class<Model> getModelClass() {
    return Model.class;
  }

  @Nested
  class LeftCompareExpression {

    @Test
    void withAttributeName() {
      final String filterExpression = "RangeKey < :val";
      final ImmutableMap<String, Object> arguments = ImmutableMap.of(":val", 10);
      final AttributeValueMapper expectedValueMapper =
          new AttributeValueMapper(
              ImmutableMap.of(
                  ":val", obj -> getDynamoDbMapperTableModel().rangeKey().convert(obj)));

      final ConditionExpressionParser mapper =
          new ConditionExpressionParser(filterExpression, getDynamoDbMapperTableModel());
      assertThat(mapper.getExpressionAttributeNames()).isEmpty();

      final AttributeValueMapper valueMapper = mapper.getAttributeValueMapper();
      assertThat(applyArguments(arguments, valueMapper))
          .isEqualTo(applyArguments(arguments, expectedValueMapper));
    }

    @Test
    void withNestedPath() {
      final String filterExpression = "A.B[1].C < :val";
      final ImmutableMap<String, Object> arguments = ImmutableMap.of(":val", 10);
      final AttributeValueMapper expectedValueMapper =
          new AttributeValueMapper(
              ImmutableMap.of(":val", obj -> new AttributeValue().withN("10")));

      final ConditionExpressionParser mapper =
          new ConditionExpressionParser(filterExpression, getDynamoDbMapperTableModel());
      assertThat(mapper.getExpressionAttributeNames()).isEmpty();

      final AttributeValueMapper valueMapper = mapper.getAttributeValueMapper();
      assertThat(applyArguments(arguments, valueMapper))
          .isEqualTo(applyArguments(arguments, expectedValueMapper));
    }

    @Test
    void withExpressionAttributeName() {
      final String filterExpression = "#nameHolder < :val";
      final ImmutableMap<String, Object> arguments = ImmutableMap.of(":val", 10);
      final AttributeValueMapper expectedValueMapper =
          new AttributeValueMapper(
              ImmutableMap.of(
                  ":val", obj -> getDynamoDbMapperTableModel().rangeKey().convert(obj)));

      final ConditionExpressionParser mapper =
          new ConditionExpressionParser(filterExpression, getDynamoDbMapperTableModel());
      assertThat(mapper.getExpressionAttributeNames()).containsOnly("#nameHolder");

      final AttributeNameMapper nameMapper = mapper.getAttributeNameMapper();
      final AttributeValueMapper valueMapper =
          nameMapper.toValueMapper(ImmutableMap.of("#nameHolder", "RangeKey"));
      assertThat(applyArguments(arguments, valueMapper))
          .isEqualTo(applyArguments(arguments, expectedValueMapper));
    }
  }

  @Nested
  class RightCompareExpression {
    @Test
    void withAttributeName() {
      final String filterExpression = ":val < RangeKey";
      final ImmutableMap<String, Object> arguments = ImmutableMap.of(":val", 10);
      final AttributeValueMapper expectedValueMapper =
          new AttributeValueMapper(
              ImmutableMap.of(
                  ":val", obj -> getDynamoDbMapperTableModel().rangeKey().convert(obj)));

      final ConditionExpressionParser mapper =
          new ConditionExpressionParser(filterExpression, getDynamoDbMapperTableModel());
      assertThat(mapper.getExpressionAttributeNames()).isEmpty();

      final AttributeValueMapper valueMapper = mapper.getAttributeValueMapper();
      assertThat(applyArguments(arguments, valueMapper))
          .isEqualTo(applyArguments(arguments, expectedValueMapper));
    }

    @Test
    void withNestedPath() {
      final String filterExpression = ":val < A.B[1].C";
      final ImmutableMap<String, Object> arguments = ImmutableMap.of(":val", 10);
      final AttributeValueMapper expectedValueMapper =
          new AttributeValueMapper(
              ImmutableMap.of(":val", obj -> new AttributeValue().withN("10")));

      final ConditionExpressionParser mapper =
          new ConditionExpressionParser(filterExpression, getDynamoDbMapperTableModel());
      assertThat(mapper.getExpressionAttributeNames()).isEmpty();

      final AttributeValueMapper valueMapper = mapper.getAttributeValueMapper();
      assertThat(applyArguments(arguments, valueMapper))
          .isEqualTo(applyArguments(arguments, expectedValueMapper));
    }

    @Test
    void withExpressionAttributeName() {
      final String filterExpression = ":val < #nameHolder";
      final ImmutableMap<String, Object> arguments = ImmutableMap.of(":val", 10);
      final AttributeValueMapper expectedValueMapper =
          new AttributeValueMapper(
              ImmutableMap.of(
                  ":val", obj -> getDynamoDbMapperTableModel().rangeKey().convert(obj)));

      final ConditionExpressionParser mapper =
          new ConditionExpressionParser(filterExpression, getDynamoDbMapperTableModel());
      assertThat(mapper.getExpressionAttributeNames()).containsOnly("#nameHolder");

      final AttributeNameMapper nameMapper = mapper.getAttributeNameMapper();
      final AttributeValueMapper valueMapper =
          nameMapper.toValueMapper(ImmutableMap.of("#nameHolder", "RangeKey"));
      assertThat(applyArguments(arguments, valueMapper))
          .isEqualTo(applyArguments(arguments, expectedValueMapper));
    }
  }

  @Nested
  class BetweenExpression {
    @Test
    void withAttributeName() {
      final String filterExpression = "RangeKey between :lower and :upper";
      final ImmutableMap<String, Object> arguments = ImmutableMap.of(":lower", 10, ":upper", 20);
      final AttributeValueMapper expectedValueMapper =
          new AttributeValueMapper(
              ImmutableMap.of(
                  ":lower", obj -> new AttributeValue().withN("10"),
                  ":upper", obj -> new AttributeValue().withN("20")));

      final ConditionExpressionParser mapper =
          new ConditionExpressionParser(filterExpression, getDynamoDbMapperTableModel());
      assertThat(mapper.getExpressionAttributeNames()).isEmpty();

      final AttributeValueMapper valueMapper = mapper.getAttributeValueMapper();
      assertThat(applyArguments(arguments, valueMapper))
          .isEqualTo(applyArguments(arguments, expectedValueMapper));
    }

    @Test
    void withNestedPath() {
      final String filterExpression = "A.B[1] between :lower and :upper";
      final ImmutableMap<String, Object> arguments = ImmutableMap.of(":lower", 10, ":upper", 20);
      final AttributeValueMapper expectedValueMapper =
          new AttributeValueMapper(
              ImmutableMap.of(
                  ":lower", obj -> new AttributeValue().withN("10"),
                  ":upper", obj -> new AttributeValue().withN("20")));

      final ConditionExpressionParser mapper =
          new ConditionExpressionParser(filterExpression, getDynamoDbMapperTableModel());
      assertThat(mapper.getExpressionAttributeNames()).isEmpty();

      final AttributeValueMapper valueMapper = mapper.getAttributeValueMapper();
      assertThat(applyArguments(arguments, valueMapper))
          .isEqualTo(applyArguments(arguments, expectedValueMapper));
    }

    @Test
    void withExpressionAttributeName() {
      final String filterExpression = "#nameHolder between :val1 and :val2";
      final ImmutableMap<String, Object> arguments = ImmutableMap.of(":val1", 10, ":val2", 20);
      final AttributeValueMapper expectedValueMapper =
          new AttributeValueMapper(
              ImmutableMap.of(
                  ":val1", obj -> getDynamoDbMapperTableModel().rangeKey().convert(obj),
                  ":val2", obj -> getDynamoDbMapperTableModel().rangeKey().convert(obj)));

      final ConditionExpressionParser mapper =
          new ConditionExpressionParser(filterExpression, getDynamoDbMapperTableModel());
      assertThat(mapper.getExpressionAttributeNames()).containsExactly("#nameHolder");

      final AttributeNameMapper nameMapper = mapper.getAttributeNameMapper();
      final AttributeValueMapper valueMapper =
          nameMapper.toValueMapper(ImmutableMap.of("#nameHolder", "RangeKey"));
      assertThat(applyArguments(arguments, valueMapper))
          .isEqualTo(applyArguments(arguments, expectedValueMapper));
    }
  }

  @Nested
  class InExpression {
    @Test
    void withAttributeName() {
      final String filterExpression = "RangeKey in (:val1, :val2)";
      final ImmutableMap<String, Object> arguments = ImmutableMap.of(":val1", 10, ":val2", 20);
      final AttributeValueMapper expectedValueMapper =
          new AttributeValueMapper(
              ImmutableMap.of(
                  ":val1", obj -> new AttributeValue().withN("10"),
                  ":val2", obj -> new AttributeValue().withN("20")));

      final ConditionExpressionParser mapper =
          new ConditionExpressionParser(filterExpression, getDynamoDbMapperTableModel());
      assertThat(mapper.getExpressionAttributeNames()).isEmpty();

      final AttributeValueMapper valueMapper = mapper.getAttributeValueMapper();
      assertThat(applyArguments(arguments, valueMapper))
          .isEqualTo(applyArguments(arguments, expectedValueMapper));
    }

    @Test
    void withNestedPath() {
      final String filterExpression = "A.B[1] in (:val1, :val2)";
      final ImmutableMap<String, Object> arguments = ImmutableMap.of(":val1", 10, ":val2", 20);
      final AttributeValueMapper expectedValueMapper =
          new AttributeValueMapper(
              ImmutableMap.of(
                  ":val1", obj -> new AttributeValue().withN("10"),
                  ":val2", obj -> new AttributeValue().withN("20")));

      final ConditionExpressionParser mapper =
          new ConditionExpressionParser(filterExpression, getDynamoDbMapperTableModel());
      assertThat(mapper.getExpressionAttributeNames()).isEmpty();

      final AttributeValueMapper valueMapper = mapper.getAttributeValueMapper();
      assertThat(applyArguments(arguments, valueMapper))
          .isEqualTo(applyArguments(arguments, expectedValueMapper));
    }

    @Test
    void withExpressionAttributeName() {
      final String filterExpression = "#nameHolder in (:val1, :val2)";
      final ImmutableMap<String, Object> arguments = ImmutableMap.of(":val1", 10, ":val2", 20);
      final AttributeValueMapper expectedValueMapper =
          new AttributeValueMapper(
              ImmutableMap.of(
                  ":val1", obj -> getDynamoDbMapperTableModel().rangeKey().convert(obj),
                  ":val2", obj -> getDynamoDbMapperTableModel().rangeKey().convert(obj)));

      final ConditionExpressionParser mapper =
          new ConditionExpressionParser(filterExpression, getDynamoDbMapperTableModel());
      assertThat(mapper.getExpressionAttributeNames()).containsOnly("#nameHolder");

      final AttributeNameMapper nameMapper = mapper.getAttributeNameMapper();
      final AttributeValueMapper valueMapper =
          nameMapper.toValueMapper(ImmutableMap.of("#nameHolder", "RangeKey"));
      assertThat(applyArguments(arguments, valueMapper))
          .isEqualTo(applyArguments(arguments, expectedValueMapper));
    }
  }

  @Nested
  class AttributeTypeFunction {
    @Test
    void withAttributeName() {
      final String filterExpression = "attribute_type(RangeKey, :type)";
      final ImmutableMap<String, Object> arguments = ImmutableMap.of(":type", "Number");
      final AttributeValueMapper expectedValueMapper =
          new AttributeValueMapper(
              ImmutableMap.of(":type", obj -> new AttributeValue().withS("Number")));

      final ConditionExpressionParser mapper =
          new ConditionExpressionParser(filterExpression, getDynamoDbMapperTableModel());
      assertThat(mapper.getExpressionAttributeNames()).isEmpty();

      final AttributeValueMapper valueMapper = mapper.getAttributeValueMapper();
      assertThat(applyArguments(arguments, valueMapper))
          .isEqualTo(applyArguments(arguments, expectedValueMapper));
    }

    @Test
    void withNestedAttributeName() {
      final String filterExpression = "attribute_type(A.B, :type)";
      final ImmutableMap<String, Object> arguments = ImmutableMap.of(":type", "Number");
      final AttributeValueMapper expectedValueMapper =
          new AttributeValueMapper(
              ImmutableMap.of(":type", obj -> new AttributeValue().withS("Number")));

      final ConditionExpressionParser mapper =
          new ConditionExpressionParser(filterExpression, getDynamoDbMapperTableModel());
      assertThat(mapper.getExpressionAttributeNames()).isEmpty();

      final AttributeValueMapper valueMapper = mapper.getAttributeValueMapper();
      assertThat(applyArguments(arguments, valueMapper))
          .isEqualTo(applyArguments(arguments, expectedValueMapper));
    }
  }

  @Nested
  class BeginsWithFunction {
    @Test
    void withAttributeName() {
      final String filterExpression = "begins_with(StringAttribute, :val)";
      final ImmutableMap<String, Object> arguments = ImmutableMap.of(":val", "Abc");
      final AttributeValueMapper expectedValueMapper =
          new AttributeValueMapper(
              ImmutableMap.of(":val", obj -> new AttributeValue().withS("Abc")));

      final ConditionExpressionParser mapper =
          new ConditionExpressionParser(filterExpression, getDynamoDbMapperTableModel());
      assertThat(mapper.getExpressionAttributeNames()).isEmpty();

      final AttributeValueMapper valueMapper = mapper.getAttributeValueMapper();
      assertThat(applyArguments(arguments, valueMapper))
          .isEqualTo(applyArguments(arguments, expectedValueMapper));
    }

    @Test
    void withNestedAttributeName() {
      final String filterExpression = "begins_with(A[1].StringAttribute, :val)";
      final ImmutableMap<String, Object> arguments = ImmutableMap.of(":val", "Abc");
      final AttributeValueMapper expectedValueMapper =
          new AttributeValueMapper(
              ImmutableMap.of(":val", obj -> new AttributeValue().withS("Abc")));

      final ConditionExpressionParser mapper =
          new ConditionExpressionParser(filterExpression, getDynamoDbMapperTableModel());
      assertThat(mapper.getExpressionAttributeNames()).isEmpty();

      final AttributeValueMapper valueMapper = mapper.getAttributeValueMapper();
      assertThat(applyArguments(arguments, valueMapper))
          .isEqualTo(applyArguments(arguments, expectedValueMapper));
    }
  }

  @Nested
  class ContainsFunction {
    @Test
    void withAttributeName() {
      final String filterExpression = "contains(StringAttribute, :val)";
      final ImmutableMap<String, Object> arguments = ImmutableMap.of(":val", "Abc");
      final AttributeValueMapper expectedValueMapper =
          new AttributeValueMapper(
              ImmutableMap.of(":val", obj -> new AttributeValue().withS("Abc")));

      final ConditionExpressionParser mapper =
          new ConditionExpressionParser(filterExpression, getDynamoDbMapperTableModel());
      assertThat(mapper.getExpressionAttributeNames()).isEmpty();

      final AttributeValueMapper valueMapper = mapper.getAttributeValueMapper();
      assertThat(applyArguments(arguments, valueMapper))
          .isEqualTo(applyArguments(arguments, expectedValueMapper));
    }

    @Test
    void withNestedAttributeName() {
      final String filterExpression = "contains(A.StringAttribute, :val)";
      final ImmutableMap<String, Object> arguments = ImmutableMap.of(":val", "Abc");
      final AttributeValueMapper expectedValueMapper =
          new AttributeValueMapper(
              ImmutableMap.of(":val", obj -> new AttributeValue().withS("Abc")));

      final ConditionExpressionParser mapper =
          new ConditionExpressionParser(filterExpression, getDynamoDbMapperTableModel());
      assertThat(mapper.getExpressionAttributeNames()).isEmpty();

      final AttributeValueMapper valueMapper = mapper.getAttributeValueMapper();
      assertThat(applyArguments(arguments, valueMapper))
          .isEqualTo(applyArguments(arguments, expectedValueMapper));
    }
  }

  private static List<AttributeValue> applyArguments(
      final ImmutableMap<String, Object> arguments, final AttributeValueMapper valueMapper) {
    return arguments.keySet().stream()
        .map(key -> valueMapper.get(key).convert(arguments.get(key)))
        .collect(Collectors.toList());
  }
}
