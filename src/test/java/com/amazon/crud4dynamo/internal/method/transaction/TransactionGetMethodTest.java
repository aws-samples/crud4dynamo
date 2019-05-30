package com.amazon.crud4dynamo.internal.method.transaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.annotation.transaction.Get;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.testbase.DynamoDbTestBase;
import com.amazon.crud4dynamo.testhelper.TableProvisioner;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import java.lang.reflect.Method;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class TransactionGetMethodTest extends DynamoDbTestBase {

    private static final String TABLE_NAME_1 = "TestTable1";
    private static final String TABLE_NAME_2 = "TestTable2";

    @Data
    @Builder
    @DynamoDBTable(tableName = TABLE_NAME_1)
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Table1 {
        private static final String HASH_KEY = "HashKey";
        private static final String INT_ATTRIBUTE = "IntAttribute";
        private static final String STRING_ATTRIBUTE = "StringAttribute";

        @DynamoDBHashKey(attributeName = HASH_KEY)
        private String hashKey;

        @DynamoDBAttribute(attributeName = INT_ATTRIBUTE)
        private Integer integerAttribute;

        @DynamoDBAttribute(attributeName = STRING_ATTRIBUTE)
        private String stringAttribute;
    }

    @Data
    @Builder
    @DynamoDBTable(tableName = TABLE_NAME_2)
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Table2 {
        private static final String HASH_KEY = "HashKey";
        private static final String INT_ATTRIBUTE = "IntAttribute";

        @DynamoDBHashKey(attributeName = HASH_KEY)
        private String hashKey;

        @DynamoDBAttribute(attributeName = INT_ATTRIBUTE)
        private Integer integerAttribute;
    }

    private interface TestDao {
        @Get(tableClass = Table1.class, keyExpression = "HashKey = :hashKey", projectionExpression = Table1.STRING_ATTRIBUTE)
        List<String> methodWihInvalidReturnType();

        @Get(tableClass = Table1.class, keyExpression = "HashKey = :hashKey1", projectionExpression = Table1.STRING_ATTRIBUTE)
        @Get(tableClass = Table1.class, keyExpression = "HashKey = :hashKey2", projectionExpression = Table1.STRING_ATTRIBUTE)
        List<Object> get1(@Param(":hashKey1") final String hashKey1, @Param(":hashKey2") final String hashKey2);

        @Get(tableClass = Table1.class, keyExpression = "HashKey = :hashKey", projectionExpression = Table1.STRING_ATTRIBUTE)
        @Get(tableClass = Table2.class, keyExpression = "HashKey = :hashKey", projectionExpression = Table2.INT_ATTRIBUTE)
        List<Object> get2(@Param(":hashKey") final String hashKey);
    }

    @Test
    void withInvalidReturnType_throwException() throws Exception {
        final Method method = TestDao.class.getMethod("methodWihInvalidReturnType");
        final Signature signature = Signature.resolve(method, TestDao.class);

        assertThatThrownBy(() -> new TransactionGetMethod(getDbClient(), getDbMapper(), signature))
                .isInstanceOf(TransactionGetMethod.ReturnTypeInvalidException.class);
    }

    @Nested
    class SingleTableGetTest extends DynamoDbTestBase {
        private TransactionGetMethod transaction;

        @Override
        public void setUp() throws Exception {
            super.setUp();
            new TableProvisioner(getDbClient()).create(Table1.class);
            final Method method = TestDao.class.getMethod("get1", String.class, String.class);
            final Signature signature = Signature.resolve(method, TestDao.class);
            transaction = new TransactionGetMethod(getDbClient(), getDbMapper(), signature);
        }

        @Test
        void get() throws Throwable {
            final Table1 model1 = Table1.builder().hashKey("hashKey1").integerAttribute(3).stringAttribute("abc").build();
            final Table1 model2 = Table1.builder().hashKey("hashKey2").integerAttribute(3).stringAttribute("cba").build();
            getDbMapper().save(model1);
            getDbMapper().save(model2);

            final List<Object> results = (List<Object>) transaction.invoke(model1.getHashKey(), model2.getHashKey());

            assertThat(results.get(0)).isEqualTo(Table1.builder().stringAttribute(model1.getStringAttribute()).build());
            assertThat(results.get(1)).isEqualTo(Table1.builder().stringAttribute(model2.getStringAttribute()).build());
        }

        @Test
        void get_withNonExistingKey_contains_emptyItem() throws Throwable {
            final Table1 model = Table1.builder().hashKey("hashKey1").integerAttribute(3).stringAttribute("abc").build();
            getDbMapper().save(model);

            final List<Object> results = (List<Object>) transaction.invoke(model.getHashKey(), "nonExistingKey");

            assertThat(results.get(0)).isEqualTo(Table1.builder().stringAttribute(model.getStringAttribute()).build());
            assertThat(results.get(1)).isEqualTo(Table1.builder().build());
        }
    }

    @Nested
    class MultipleTablesGetTest extends DynamoDbTestBase {
        private TransactionGetMethod transaction;

        @Override
        public void setUp() throws Exception {
            super.setUp();
            new TableProvisioner(getDbClient()).create(Table1.class);
            new TableProvisioner(getDbClient()).create(Table2.class);
            final Method method = TestDao.class.getMethod("get2", String.class);
            final Signature signature = Signature.resolve(method, TestDao.class);
            transaction = new TransactionGetMethod(getDbClient(), getDbMapper(), signature);
        }

        @Test
        void get() throws Throwable {
            final String hashKey = "hashKey";
            final Table1 model1 = Table1.builder().hashKey(hashKey).integerAttribute(3).stringAttribute("abc").build();
            final Table2 model2 = Table2.builder().hashKey(hashKey).integerAttribute(3).integerAttribute(3).build();
            getDbMapper().save(model1);
            getDbMapper().save(model2);

            final List<Object> results = (List<Object>) transaction.invoke(hashKey);

            assertThat(results.get(0)).isEqualTo(Table1.builder().stringAttribute(model1.getStringAttribute()).build());
            assertThat(results.get(1)).isEqualTo(Table2.builder().integerAttribute(model2.getIntegerAttribute()).build());
        }
    }
}
