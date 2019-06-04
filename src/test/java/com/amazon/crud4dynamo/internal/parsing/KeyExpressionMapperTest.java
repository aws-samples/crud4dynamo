package com.amazon.crud4dynamo.internal.parsing;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazon.crud4dynamo.internal.parsing.KeyExpressionMapper.Context;
import com.amazon.crud4dynamo.internal.parsing.KeyExpressionMapperTest.Model;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.ImmutableMap;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;

class KeyExpressionMapperTest extends SingleTableDynamoDbTestBase<Model> {
  @Override
  protected Class<Model> getModelClass() {
    return Model.class;
  }

  @Test
  void hashKeyContext_case1() {
    final KeyExpressionMapper mapper =
        new KeyExpressionMapper("HashKey = :value1", getDynamoDbMapperTableModel());

    final Context hashKeyContext = mapper.getHashKeyContext();

    assertThat(hashKeyContext.getKeyStringText()).isEqualTo("HashKey");
    assertThat(hashKeyContext.getNameMapper().getInnerMap()).isEmpty();
    assertThat(hashKeyContext.getValueMapper().get(":value1").convert("hashKey"))
        .isEqualTo(new AttributeValue("hashKey"));
  }

  @Test
  void hashKeyContext_case2() {
    final KeyExpressionMapper mapper =
        new KeyExpressionMapper("#hashKey = :value1", getDynamoDbMapperTableModel());

    final Context hashKeyContext = mapper.getHashKeyContext();

    assertThat(hashKeyContext.getKeyStringText()).isEqualTo("#hashKey");
    assertThat(hashKeyContext.getNameMapper().getInnerMap()).containsKey("#hashKey");
    assertThat(hashKeyContext.getValueMapper().get(":value1")).isNull();

    final AttributeValueConverter converter =
        hashKeyContext
            .getNameMapper()
            .toValueMapper(ImmutableMap.of("#hashKey", "HashKey"))
            .get(":value1");

    assertThat(converter).isNotNull();
    assertThat(converter.convert("hashKey")).isEqualTo(new AttributeValue("hashKey"));
  }

  @Test
  void rangeKeyContext_case1() {
    final KeyExpressionMapper mapper =
        new KeyExpressionMapper(
            "HashKey = :value1, RangeKey = :value2", getDynamoDbMapperTableModel());

    final Optional<Context> context = mapper.getRangeKeyContext();

    assertThat(context).isPresent();

    assertThat(context.get().getKeyStringText()).isEqualTo("RangeKey");
    assertThat(context.get().getNameMapper().getInnerMap()).isEmpty();
    assertThat(context.get().getValueMapper().get(":value2").convert(100))
        .isEqualTo(new AttributeValue().withN("100"));
  }

  @Test
  void rangeKeyContext_case2() {
    final KeyExpressionMapper mapper =
        new KeyExpressionMapper(
            "HashKey = :value1, #rangeKey = :value2", getDynamoDbMapperTableModel());

    final Optional<Context> context = mapper.getRangeKeyContext();

    assertThat(context).isPresent();

    assertThat(context.get().getKeyStringText()).isEqualTo("#rangeKey");
    assertThat(context.get().getNameMapper().getInnerMap()).containsKey("#rangeKey");
    assertThat(context.get().getValueMapper().get(":value2")).isNull();

    final AttributeValueConverter converter =
        context
            .get()
            .getNameMapper()
            .toValueMapper(ImmutableMap.of("#rangeKey", "RangeKey"))
            .get(":value2");

    assertThat(converter).isNotNull();
    assertThat(converter.convert(123)).isEqualTo(new AttributeValue().withN("123"));
  }

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
  }
}
