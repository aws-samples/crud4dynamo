package com.amazon.crud4dynamo.internal.method.transaction;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.annotation.transaction.Update;
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
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;

class UpdateFactoryTest extends SingleTableDynamoDbTestBase<UpdateFactoryTest.Table> {
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
        String UPDATE_EXPRESSION = "SET #attribute = :newValue";
        String CONDITION_EXPRESSION = "#attribute = :oldValue";

        @Update(
                tableClass = Table.class,
                keyExpression = "HashKey = :hashKey",
                updateExpression = UPDATE_EXPRESSION,
                conditionExpression = CONDITION_EXPRESSION)
        void update(
                @Param(":hashKey") final String hashKey,
                @Param("#attribute") final String attribute,
                @Param(":newValue") final String newValue,
                @Param(":oldValue") final String oldValue);
    }

    @Test
    void update() throws Exception {
        final Method method = Dao.class.getMethod("update", String.class, String.class, String.class, String.class);
        final Signature signature = Signature.resolve(method, Dao.class);
        final UpdateFactory factory = new UpdateFactory(signature.getAnnotation(Update.class).get(), getDynamoDbMapperTableModel());
        final String hashKeyValue = "hashKeyValue";
        final String oldValue = "oldValue";
        final String newValue = "newValue";

        final com.amazonaws.services.dynamodbv2.model.Update update =
                factory.create(
                        Argument.newList(signature.parameters(), Arrays.asList(hashKeyValue, Table.STRING_ATTRIBUTE, newValue, oldValue)));

        assertThat(update.getTableName()).isEqualTo(Table.NAME);
        assertThat(update.getKey()).containsEntry(Table.HASH_KEY, new AttributeValue(hashKeyValue)).hasSize(1);
        assertThat(update.getUpdateExpression()).isEqualTo(Dao.UPDATE_EXPRESSION);
        assertThat(update.getConditionExpression()).isEqualTo(Dao.CONDITION_EXPRESSION);
        assertThat(update.getExpressionAttributeNames()).containsEntry("#attribute", Table.STRING_ATTRIBUTE);
        assertThat(update.getExpressionAttributeValues())
                .containsEntry(":oldValue", new AttributeValue(oldValue))
                .containsEntry(":newValue", new AttributeValue(newValue));
        assertThat(update.getReturnValuesOnConditionCheckFailure()).isEqualTo(ReturnValuesOnConditionCheckFailure.NONE.toString());
    }
}
