package com.amazon.crud4dynamo.internal.method.transaction;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.annotation.transaction.Delete;
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

import static org.assertj.core.api.Assertions.assertThat;

class DeleteFactoryTest extends SingleTableDynamoDbTestBase<DeleteFactoryTest.Table> {
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
        String CONDITION_EXPRESSION = "begins_with(#attribute, :prefix)";

        @Delete(tableClass = Table.class, keyExpression = "HashKey = :hashKey", conditionExpression = CONDITION_EXPRESSION)
        void delete(
                @Param(":hashKey") final String hashKeyValue,
                @Param("#attribute") final String attributeName,
                @Param(":prefix") final String prefix);
    }

    @Test
    void delete() throws Exception {
        final Method deleteMethod = Dao.class.getMethod("delete", String.class, String.class, String.class);
        final Signature signature = Signature.resolve(deleteMethod, Dao.class);
        final DeleteFactory factory = new DeleteFactory(signature.getAnnotation(Delete.class).get(), getDynamoDbMapperTableModel());
        final String hashKeyValue = "hashKeyValue";
        final String prefix = "prefix";

        final com.amazonaws.services.dynamodbv2.model.Delete delete =
                factory.create(Argument.newList(signature.parameters(), Arrays.asList(hashKeyValue, Table.STRING_ATTRIBUTE, prefix)));

        assertThat(delete.getTableName()).isEqualTo(Table.NAME);
        assertThat(delete.getKey()).containsEntry(Table.HASH_KEY, new AttributeValue(hashKeyValue)).hasSize(1);
        assertThat(delete.getConditionExpression()).isEqualTo(Dao.CONDITION_EXPRESSION);
        assertThat(delete.getExpressionAttributeNames()).containsEntry("#attribute", Table.STRING_ATTRIBUTE);
        assertThat(delete.getExpressionAttributeValues()).containsEntry(":prefix", new AttributeValue(prefix));
        assertThat(delete.getReturnValuesOnConditionCheckFailure()).isEqualTo(ReturnValuesOnConditionCheckFailure.NONE.toString());
    }
}
