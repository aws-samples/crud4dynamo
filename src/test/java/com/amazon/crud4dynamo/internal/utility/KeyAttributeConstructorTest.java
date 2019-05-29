package com.amazon.crud4dynamo.internal.utility;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.extension.Argument;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.local.shared.access.AmazonDynamoDBLocal;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.Data;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KeyAttributeConstructorTest {

    private AmazonDynamoDBLocal amazonDynamoDBLocal;
    private DynamoDBMapper dynamoDBMapper;

    private static final String HASH_KEY = "HashKey";
    private static final String RANGE_KEY = "RangeKey";

    @Data
    private static class SimpleKeyModel {
        @DynamoDBHashKey(attributeName = HASH_KEY)
        private String hashKey;
    }

    @Data
    private static class CompositeKeyModel {
        @DynamoDBHashKey(attributeName = HASH_KEY)
        private String hashKey;

        @DynamoDBHashKey(attributeName = RANGE_KEY)
        private String rangeKey;
    }

    private interface Dao {
        void emptyArgumentMethod();

        void validSimpleKeyMethod(@Param("#hashKey") final String hashKey, @Param(":hashKeyValue") final String hashKeyValue);

        void validCompositeKeyMethod(
                @Param("#hashKey") final String hashKey,
                @Param(":hashKeyValue") final String hashKeyValue,
                @Param("#rangeKey") final String rangeKey,
                @Param(":rangeKey") final String rangeKeyValue);
    }

    @BeforeEach
    void setUp() {
        amazonDynamoDBLocal = DynamoDBEmbedded.create();
        dynamoDBMapper = new DynamoDBMapper(amazonDynamoDBLocal.amazonDynamoDB());
    }

    @Test
    void noAttributeValue_throwException() throws Exception {
        final Signature method = Signature.resolve(Dao.class.getMethod("emptyArgumentMethod"), Dao.class);
        final List<Argument> arguments = Argument.newList(method.parameters(), Collections.emptyList());
        final KeyAttributeConstructor constructor =
                new KeyAttributeConstructor("#hashKey = :hashKeyValue", dynamoDBMapper.getTableModel(SimpleKeyModel.class));

        assertThatThrownBy(() -> constructor.create(arguments)).isInstanceOf(KeyAttributeConstructor.NoKeyAttributeException.class);
    }

    @Test
    void createSimpleKeyAttribute() throws Exception {
        final Signature method = Signature.resolve(Dao.class.getMethod("validSimpleKeyMethod", String.class, String.class), Dao.class);
        final String value = "dummyValue";
        final List<Argument> arguments = Argument.newList(method.parameters(), Arrays.asList(HASH_KEY, value));
        final KeyAttributeConstructor constructor =
                new KeyAttributeConstructor("#hashKey = :hashKeyValue", dynamoDBMapper.getTableModel(SimpleKeyModel.class));

        final Map<String, AttributeValue> keyAttribute = constructor.create(arguments);

        assertThat(keyAttribute).containsEntry(HASH_KEY, new AttributeValue(value)).hasSize(1);
    }

    @Test
    void createCompositeKeyAttributes() throws Exception {
        final Signature method =
                Signature.resolve(
                        Dao.class.getMethod("validCompositeKeyMethod", String.class, String.class, String.class, String.class), Dao.class);
        final String value1 = "dummyValue1";
        final String value2 = "dummyValue2";
        final List<Argument> arguments = Argument.newList(method.parameters(), Arrays.asList(HASH_KEY, value1, RANGE_KEY, value2));
        final KeyAttributeConstructor constructor =
                new KeyAttributeConstructor(
                        "#hashKey = :hashKeyValue, #rangeKey = :rangeKey", dynamoDBMapper.getTableModel(CompositeKeyModel.class));

        final Map<String, AttributeValue> keyAttribute = constructor.create(arguments);

        assertThat(keyAttribute)
                .containsEntry(HASH_KEY, new AttributeValue(value1))
                .containsEntry(RANGE_KEY, new AttributeValue(value2))
                .hasSize(2);
    }
}
