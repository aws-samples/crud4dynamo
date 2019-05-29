package com.amazon.crud4dynamo.internal.method.transaction;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.annotation.transaction.Put;
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

class PutFactoryTest extends SingleTableDynamoDbTestBase<PutFactoryTest.Table> {
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

        @Put(tableClass = Table.class, item = ":item", conditionExpression = CONDITION_EXPRESSION)
        void put(@Param(":item") final Table item, @Param("#attribute") final String attributeName, @Param(":prefix") final String prefix);
    }

    @Test
    void put() throws Exception {
        final Method deleteMethod = Dao.class.getMethod("put", Table.class, String.class, String.class);
        final Signature signature = Signature.resolve(deleteMethod, Dao.class);
        final PutFactory factory = new PutFactory(signature.getAnnotation(Put.class).get(), getDynamoDbMapperTableModel());
        final String prefix = "prefix";
        final String hashKey = "hashKey";
        final Table table = Table.builder().hashKey(hashKey).build();

        final com.amazonaws.services.dynamodbv2.model.Put put =
                factory.create(Argument.newList(signature.parameters(), Arrays.asList(table, Table.STRING_ATTRIBUTE, prefix)));

        assertThat(put.withTableName(Table.NAME));
        assertThat(put.getItem()).containsEntry(Table.HASH_KEY, new AttributeValue(hashKey)).hasSize(1);
        assertThat(put.getConditionExpression()).isEqualTo(Dao.CONDITION_EXPRESSION);
        assertThat(put.getExpressionAttributeNames()).containsEntry("#attribute", Table.STRING_ATTRIBUTE).hasSize(1);
        assertThat(put.getExpressionAttributeValues()).containsEntry(":prefix", new AttributeValue(prefix)).hasSize(1);
        assertThat(put.getReturnValuesOnConditionCheckFailure()).isEqualTo(ReturnValuesOnConditionCheckFailure.NONE.toString());
    }
}
