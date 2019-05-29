package com.amazon.crud4dynamo.internal.method.transaction;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.annotation.transaction.ConditionCheck;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.testbase.DynamoDbTestBase;
import com.amazon.crud4dynamo.testhelper.TableProvisioner;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.TransactionCanceledException;
import java.lang.reflect.Method;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TransactionWriteMethodWithConditionCheckOnlyTest {
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
        @ConditionCheck(
                tableClass = Table1.class,
                conditionExpression = "attribute_exists(#attribute)",
                keyExpression = "HashKey = :hashKey")
        void checkOnTable1(@Param(":hashKey") final String hashKey, @Param("#attribute") final String attribute);

        @ConditionCheck(
                tableClass = Table1.class,
                conditionExpression = "attribute_exists(#attribute)",
                keyExpression = "HashKey = :hashKey")
        @ConditionCheck(
                tableClass = Table2.class,
                conditionExpression = "attribute_exists(#attribute)",
                keyExpression = "HashKey = :hashKey")
        void checkOnTable_1_and_2(@Param(":hashKey") final String hashKey, @Param("#attribute") final String attribute);
    }

    @Nested
    class SingleConditionCheckTest extends DynamoDbTestBase {

        private TransactionWriteMethod transaction;

        @BeforeEach
        public void setUp() throws Exception {
            super.setUp();
            new TableProvisioner(getDbClient()).create(Table1.class);
            final Method method = TestDao.class.getMethod("checkOnTable1", String.class, String.class);
            final Signature signature = Signature.resolve(method, TestDao.class);
            transaction = new TransactionWriteMethod(getDbClient(), getDbMapper(), signature);
        }

        @Test
        void attributeNotExist_throwException() {
            final String aHashKey = "aHashKey";
            getDbMapper().save(Table1.builder().hashKey(aHashKey).build());

            assertThatThrownBy(() -> transaction.invoke(aHashKey, Table1.INT_ATTRIBUTE)).isInstanceOf(TransactionCanceledException.class);
        }

        @Test
        void attributeExist_succeeded() throws Throwable {
            final String aHashKey = "aHashKey";
            getDbMapper().save(Table1.builder().hashKey(aHashKey).integerAttribute(1).build());

            transaction.invoke(aHashKey, Table1.INT_ATTRIBUTE);
        }
    }

    @Nested
    class MultipleConditionCheckTest extends DynamoDbTestBase {

        private TransactionWriteMethod transactionWriteMethod;

        @BeforeEach
        public void setUp() throws Exception {
            super.setUp();
            new TableProvisioner(getDbClient()).create(Table1.class);
            new TableProvisioner(getDbClient()).create(Table2.class);
            final Method method = TestDao.class.getMethod("checkOnTable_1_and_2", String.class, String.class);
            final Signature signature = Signature.resolve(method, TestDao.class);
            transactionWriteMethod = new TransactionWriteMethod(getDbClient(), getDbMapper(), signature);
        }

        @Test
        void transactionSucceeded() throws Throwable {
            final String aHashKey = "aHashKey";
            getDbMapper().save(Table1.builder().hashKey(aHashKey).integerAttribute(1).build());
            getDbMapper().save(Table2.builder().hashKey(aHashKey).integerAttribute(1).build());

            transactionWriteMethod.invoke(aHashKey, Table1.INT_ATTRIBUTE);
        }
    }
}
