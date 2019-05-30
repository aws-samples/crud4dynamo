package com.amazon.crud4dynamo.internal.parsing;

import static org.assertj.core.api.Assertions.assertThat;

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

class KeyConditionExpressionParserTest {

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @DynamoDBTable(tableName = "TestTable")
    static class ModelWithIntegerRangeKey {
        @DynamoDBHashKey(attributeName = "HashKey")
        private String hashKey;

        @DynamoDBRangeKey(attributeName = "RangeKey")
        private Integer rangeKey;
    }

    @Nested
    class CompareExpression extends SingleTableDynamoDbTestBase<ModelWithIntegerRangeKey> {

        private final ImmutableMap<String, Object> arguments = ImmutableMap.of(":hashKey", "hashkey", ":lower", 10);
        private final AttributeValueMapper expectedValueMapper = new AttributeValueMapper();

        {
            expectedValueMapper.put(":hashKey", obj -> getDynamoDbMapperTableModel().hashKey().convert(obj));
            expectedValueMapper.put(":lower", obj -> getDynamoDbMapperTableModel().rangeKey().convert(obj));
        }

        @Override
        protected Class<ModelWithIntegerRangeKey> getModelClass() {
            return ModelWithIntegerRangeKey.class;
        }

        @Test
        void withAttributeName() {
            final String keyCondition = "HashKey = :hashKey AND RangeKey > :lower";

            final KeyConditionExpressionParser mapper = new KeyConditionExpressionParser(keyCondition, getDynamoDbMapperTableModel());

            assertThat(mapper.getExpressionAttributeNames()).isEmpty();
            verify(mapper.getAttributeValueMapper(), arguments, expectedValueMapper);
        }

        private void verify(
                final AttributeValueMapper valueMapper,
                final ImmutableMap<String, Object> arguments,
                final AttributeValueMapper expectedValueMapper) {
            assertThat(applyArguments(arguments, valueMapper)).isEqualTo(applyArguments(arguments, expectedValueMapper));
        }

        @Test
        void withExpressionAttributeName() {
            final String keyCondition = "#HashKey = :hashKey AND #RangeKey > :lower";

            final KeyConditionExpressionParser mapper = new KeyConditionExpressionParser(keyCondition, getDynamoDbMapperTableModel());
            assertThat(mapper.getExpressionAttributeNames()).containsOnly("#HashKey", "#RangeKey");

            final AttributeValueMapper valueMapper =
                    mapper.getAttributeNameMapper().toValueMapper(ImmutableMap.of("#HashKey", "HashKey", "#RangeKey", "RangeKey"));
            verify(valueMapper, arguments, expectedValueMapper);
        }
    }

    @Nested
    class BetweenExpression extends SingleTableDynamoDbTestBase<ModelWithIntegerRangeKey> {

        private final ImmutableMap<String, Object> arguments = ImmutableMap.of(":hashKey", "hashkey", ":lower", 10, ":upper", 20);
        private final AttributeValueMapper expectedValueMapper = new AttributeValueMapper();

        {
            expectedValueMapper.put(":hashKey", obj -> getDynamoDbMapperTableModel().hashKey().convert(obj));
            expectedValueMapper.put(":lower", obj -> getDynamoDbMapperTableModel().rangeKey().convert(obj));
            expectedValueMapper.put(":upper", obj -> getDynamoDbMapperTableModel().rangeKey().convert(obj));
        }

        @Override
        protected Class<ModelWithIntegerRangeKey> getModelClass() {
            return ModelWithIntegerRangeKey.class;
        }

        @Test
        void withAttributeName() {
            final String keyCondition = "HashKey = :hashKey AND RangeKey BETWEEN :lower AND :upper";

            final KeyConditionExpressionParser mapper = new KeyConditionExpressionParser(keyCondition, getDynamoDbMapperTableModel());
            final AttributeValueMapper valueMapper = mapper.getAttributeValueMapper();

            assertThat(mapper.getExpressionAttributeNames()).isEmpty();
            verify(valueMapper, arguments, expectedValueMapper);
        }

        private void verify(
                final AttributeValueMapper valueMapper,
                final ImmutableMap<String, Object> arguments,
                final AttributeValueMapper expectedValueMapper) {
            assertThat(applyArguments(arguments, valueMapper)).isEqualTo(applyArguments(arguments, expectedValueMapper));
        }

        @Test
        void withExpressionAttributeName() {
            final String keyCondition = "#HashKey = :hashKey AND #RangeKey BETWEEN :lower AND :upper";

            final KeyConditionExpressionParser mapper = new KeyConditionExpressionParser(keyCondition, getDynamoDbMapperTableModel());
            assertThat(mapper.getExpressionAttributeNames()).containsOnly("#HashKey", "#RangeKey");

            final AttributeValueMapper valueMapper =
                    mapper.getAttributeNameMapper().toValueMapper(ImmutableMap.of("#HashKey", "HashKey", "#RangeKey", "RangeKey"));
            verify(valueMapper, arguments, expectedValueMapper);
        }
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @DynamoDBTable(tableName = "TestTable")
    static class ModelWithStringRangeKey {
        @DynamoDBHashKey(attributeName = "HashKey")
        private String hashKey;

        @DynamoDBRangeKey(attributeName = "RangeKey")
        private String rangeKey;
    }

    @Nested
    class BeginsWithExpression extends SingleTableDynamoDbTestBase<ModelWithStringRangeKey> {

        private final ImmutableMap<String, Object> arguments = ImmutableMap.of(":hashKey", "hashkey", ":prefix", "Prefix");
        private final AttributeValueMapper expectedValueMapper = new AttributeValueMapper();

        {
            expectedValueMapper.put(":hashKey", obj -> getDynamoDbMapperTableModel().hashKey().convert(obj));
            expectedValueMapper.put(":prefix", obj -> getDynamoDbMapperTableModel().rangeKey().convert(obj));
        }

        @Override
        protected Class<ModelWithStringRangeKey> getModelClass() {
            return ModelWithStringRangeKey.class;
        }

        @Test
        void withAttributeName() {
            final String keyCondition = "HashKey = :hashKey AND begins_with(RangeKey, :prefix)";

            final KeyConditionExpressionParser mapper = new KeyConditionExpressionParser(keyCondition, getDynamoDbMapperTableModel());
            assertThat(mapper.getExpressionAttributeNames()).isEmpty();

            final AttributeValueMapper valueMapper = mapper.getAttributeValueMapper();
            verify(valueMapper, arguments, expectedValueMapper);
        }

        private void verify(
                final AttributeValueMapper valueMapper,
                final ImmutableMap<String, Object> arguments,
                final AttributeValueMapper expectedValueMapper) {
            assertThat(applyArguments(arguments, valueMapper)).isEqualTo(applyArguments(arguments, expectedValueMapper));
        }

        @Test
        void withExpressionAttributeName() {
            final String keyCondition = "#HashKey = :hashKey AND begins_with(#RangeKey, :prefix)";

            final KeyConditionExpressionParser mapper = new KeyConditionExpressionParser(keyCondition, getDynamoDbMapperTableModel());
            assertThat(mapper.getExpressionAttributeNames()).containsOnly("#HashKey", "#RangeKey");

            final AttributeValueMapper valueMapper =
                    mapper.getAttributeNameMapper().toValueMapper(ImmutableMap.of("#HashKey", "HashKey", "#RangeKey", "RangeKey"));
            verify(valueMapper, arguments, expectedValueMapper);
        }
    }

    private static List<AttributeValue> applyArguments(
            final ImmutableMap<String, Object> arguments, final AttributeValueMapper valueMapper) {
        return arguments.keySet().stream().map(key -> valueMapper.get(key).convert(arguments.get(key))).collect(Collectors.toList());
    }
}
