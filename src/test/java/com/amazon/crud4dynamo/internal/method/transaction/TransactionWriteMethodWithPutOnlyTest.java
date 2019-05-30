package com.amazon.crud4dynamo.internal.method.transaction;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.annotation.transaction.Put;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.testbase.DynamoDbTestBase;
import com.amazon.crud4dynamo.testhelper.TableProvisioner;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import java.lang.reflect.Method;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class TransactionWriteMethodWithPutOnlyTest {
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

        @DynamoDBHashKey(attributeName = HASH_KEY)
        private String hashKey;

        @DynamoDBAttribute(attributeName = INT_ATTRIBUTE)
        private Integer integerAttribute;
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
        @Put(tableClass = Table1.class, item = ":putItem", conditionExpression = "#attribute = :oldValue")
        void put1(
                @Param(":putItem") final Table1 newItem,
                @Param("#attribute") final String attribute,
                @Param(":oldValue") final int oldValue);

        @Put(tableClass = Table1.class, item = ":putItem1", conditionExpression = "#attribute = :oldValue")
        @Put(tableClass = Table2.class, item = ":putItem2", conditionExpression = "#attribute = :oldValue")
        void put2(
                @Param(":putItem1") final Table1 newItem1,
                @Param(":putItem2") final Table1 newItem2,
                @Param("#attribute") final String attribute,
                @Param(":oldValue") final int oldValue);
    }

    @Nested
    class SingleTablePutTest extends DynamoDbTestBase {
        private TransactionWriteMethod transaction;

        @Override
        @BeforeEach
        public void setUp() throws Exception {
            super.setUp();
            new TableProvisioner(getDbClient()).create(Table1.class);
            final Method method = TestDao.class.getMethod("put1", Table1.class, String.class, int.class);
            final Signature signature = Signature.resolve(method, TestDao.class);
            transaction = new TransactionWriteMethod(getDbClient(), getDbMapper(), signature);
        }

        @Test
        void put() throws Throwable {
            final String hashKey = "hashKey";
            final int oldValue = 5;
            final int newValue = 10;
            final Table1 oldModel = Table1.builder().hashKey(hashKey).integerAttribute(oldValue).build();
            final Table1 newModel = Table1.builder().hashKey(hashKey).integerAttribute(newValue).build();
            getDbMapper().save(oldModel);

            transaction.invoke(newModel, Table1.INT_ATTRIBUTE, oldValue);

            assertThat(getDbMapper().load(oldModel)).isEqualTo(newModel);
        }
    }

    @Nested
    class MultipleTablesPutTest extends DynamoDbTestBase {
        private TransactionWriteMethod transaction;

        @Override
        @BeforeEach
        public void setUp() throws Exception {
            super.setUp();
            new TableProvisioner(getDbClient()).create(Table1.class);
            new TableProvisioner(getDbClient()).create(Table2.class);
            final Method method = TestDao.class.getMethod("put2", Table1.class, Table1.class, String.class, int.class);
            final Signature signature = Signature.resolve(method, TestDao.class);
            transaction = new TransactionWriteMethod(getDbClient(), getDbMapper(), signature);
        }

        @Test
        void put() throws Throwable {
            final String hashKey = "hashKey";
            final int oldValue = 5;
            final int newValue = 10;
            final Table1 oldModel1 = Table1.builder().hashKey(hashKey).integerAttribute(oldValue).build();
            final Table1 newModel1 = Table1.builder().hashKey(hashKey).integerAttribute(newValue).build();
            final Table2 oldModel2 = Table2.builder().hashKey(hashKey).integerAttribute(oldValue).build();
            final Table2 newModel2 = Table2.builder().hashKey(hashKey).integerAttribute(newValue).build();
            getDbMapper().save(oldModel1);
            getDbMapper().save(oldModel2);

            transaction.invoke(newModel1, newModel2, Table1.INT_ATTRIBUTE, oldValue);

            assertThat(getDbMapper().load(oldModel1)).isEqualTo(newModel1);
            assertThat(getDbMapper().load(oldModel2)).isEqualTo(newModel2);
        }
    }
}
