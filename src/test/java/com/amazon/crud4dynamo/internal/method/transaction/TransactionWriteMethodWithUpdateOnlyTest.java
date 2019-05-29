package com.amazon.crud4dynamo.internal.method.transaction;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.annotation.transaction.Update;
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

import static org.assertj.core.api.Assertions.assertThat;

public class TransactionWriteMethodWithUpdateOnlyTest {

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
        @Update(
                tableClass = Table1.class,
                keyExpression = "HashKey = :hashKey",
                updateExpression = "SET #attribute = :newValue",
                conditionExpression = "#attribute = :oldValue")
        void update1(
                @Param(":hashKey") final String hashKey,
                @Param("#attribute") final String attribute,
                @Param(":newValue") final int newValue,
                @Param(":oldValue") final int oldValue);

        @Update(
                tableClass = Table1.class,
                keyExpression = "HashKey = :hashKey",
                updateExpression = "SET #attribute = :newValue",
                conditionExpression = "#attribute = :oldValue")
        @Update(
                tableClass = Table2.class,
                keyExpression = "HashKey = :hashKey",
                updateExpression = "SET #attribute = :newValue",
                conditionExpression = "#attribute = :oldValue")
        void update2(
                @Param(":hashKey") final String hashKey,
                @Param("#attribute") final String attribute,
                @Param(":newValue") final int newValue,
                @Param(":oldValue") final int oldValue);
    }

    @Nested
    class SingleTableUpdateTest extends DynamoDbTestBase {
        private TransactionWriteMethod transaction;

        @Override
        @BeforeEach
        public void setUp() throws Exception {
            super.setUp();
            new TableProvisioner(getDbClient()).create(Table1.class);
            final Method method = TestDao.class.getMethod("update1", String.class, String.class, int.class, int.class);
            final Signature signature = Signature.resolve(method, TestDao.class);
            transaction = new TransactionWriteMethod(getDbClient(), getDbMapper(), signature);
        }

        @Test
        void update() throws Throwable {
            final Table1 oldModel = Table1.builder().hashKey("hashKey").integerAttribute(3).build();
            final Table1 newModel = Table1.builder().hashKey("hashKey").integerAttribute(5).build();
            getDbMapper().save(oldModel);

            transaction.invoke(oldModel.getHashKey(), Table1.INT_ATTRIBUTE, newModel.getIntegerAttribute(), oldModel.getIntegerAttribute());

            assertThat(getDbMapper().load(oldModel)).isEqualTo(newModel);
        }
    }

    @Nested
    class MultipleTablesUpdateTest extends DynamoDbTestBase {
        private TransactionWriteMethod transaction;

        @Override
        @BeforeEach
        public void setUp() throws Exception {
            super.setUp();
            new TableProvisioner(getDbClient()).create(Table1.class);
            new TableProvisioner(getDbClient()).create(Table2.class);
            final Method method = TestDao.class.getMethod("update2", String.class, String.class, int.class, int.class);
            final Signature signature = Signature.resolve(method, TestDao.class);
            transaction = new TransactionWriteMethod(getDbClient(), getDbMapper(), signature);
        }

        @Test
        void update() throws Throwable {
            final String hashKey = "hashKey";
            final int oldValue = 3;
            final int newValue = 5;
            final Table1 oldModel1 = Table1.builder().hashKey(hashKey).integerAttribute(oldValue).build();
            final Table1 newModel1 = Table1.builder().hashKey(hashKey).integerAttribute(newValue).build();
            final Table2 oldModel2 = Table2.builder().hashKey(hashKey).integerAttribute(oldValue).build();
            final Table2 newModel2 = Table2.builder().hashKey(hashKey).integerAttribute(newValue).build();
            getDbMapper().save(oldModel1);
            getDbMapper().save(oldModel2);

            transaction.invoke(hashKey, Table1.INT_ATTRIBUTE, newValue, oldValue);

            assertThat(getDbMapper().load(oldModel1)).isEqualTo(newModel1);
            assertThat(getDbMapper().load(oldModel2)).isEqualTo(newModel2);
        }
    }
}
