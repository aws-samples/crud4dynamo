package com.amazon.crud4dynamo.internal.method.transaction;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.annotation.transaction.Get;
import com.amazon.crud4dynamo.extension.Argument;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import java.lang.reflect.Method;
import java.util.Arrays;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;

class GetFactoryTest extends SingleTableDynamoDbTestBase<GetFactoryTest.Table> {
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
        @Get(tableClass = Table.class, keyExpression = "HashKey = :hashKey", projectionExpression = "#projectAttribute")
        void get(@Param(":hashKey") final String hashKey, @Param("#projectAttribute") final String projectAttributeName);
    }

    @Test
    void create() throws Exception {
        final Method getMethod = Dao.class.getMethod("get", String.class, String.class);
        final Signature signature = Signature.resolve(getMethod, Dao.class);
        final String hashKey = "hashKey";
        final GetFactory factory = new GetFactory(signature.getAnnotation(Get.class).get(), getDynamoDbMapperTableModel());

        final com.amazonaws.services.dynamodbv2.model.Get get =
                factory.create(Argument.newList(signature.parameters(), Arrays.asList(hashKey, Table.STRING_ATTRIBUTE)));

        assertThat(get.getTableName()).isEqualTo(Table.NAME);
        assertThat(get.getKey()).containsEntry(Table.HASH_KEY, new AttributeValue(hashKey)).hasSize(1);
        assertThat(get.getProjectionExpression()).isEqualTo("#projectAttribute");
        assertThat(get.getExpressionAttributeNames()).containsEntry("#projectAttribute", Table.STRING_ATTRIBUTE).hasSize(1);
    }
}
