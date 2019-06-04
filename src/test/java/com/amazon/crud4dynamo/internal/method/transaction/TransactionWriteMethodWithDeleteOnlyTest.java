package com.amazon.crud4dynamo.internal.method.transaction;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.annotation.transaction.Delete;
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

public class TransactionWriteMethodWithDeleteOnlyTest {
  private static final String TABLE_NAME_1 = "TestTable1";
  private static final String TABLE_NAME_2 = "TestTable2";

  private interface TestDao {
    @Delete(
        tableClass = Table1.class,
        keyExpression = "HashKey = :hashKey",
        conditionExpression = "#attributeName > :value")
    void delete1(
        @Param(":hashKey") final String key,
        @Param("#attributeName") final String attribute,
        @Param(":value") final int value);

    @Delete(
        tableClass = Table1.class,
        keyExpression = "HashKey = :hashKey",
        conditionExpression = "#attributeName > :value")
    @Delete(
        tableClass = Table2.class,
        keyExpression = "HashKey = :hashKey",
        conditionExpression = "#attributeName > :value")
    void delete2(
        @Param(":hashKey") final String key,
        @Param("#attributeName") final String attribute,
        @Param(":value") final int value);
  }

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

  @Nested
  class SingleTableDeleteTest extends DynamoDbTestBase {

    private TransactionWriteMethod transaction;

    @Override
    @BeforeEach
    public void setUp() throws Exception {
      super.setUp();
      new TableProvisioner(getDbClient()).create(Table1.class);
      final Method method =
          TestDao.class.getMethod("delete1", String.class, String.class, int.class);
      final Signature signature = Signature.resolve(method, TestDao.class);
      transaction = new TransactionWriteMethod(getDbClient(), getDbMapper(), signature);
    }

    @Test
    void delete() throws Throwable {
      final String hashKey = "hashKey";
      final Table1 model = Table1.builder().hashKey(hashKey).integerAttribute(10).build();
      getDbMapper().save(model);

      transaction.invoke(hashKey, Table1.INT_ATTRIBUTE, 5);

      assertThat(getDbMapper().load(model)).isNull();
    }
  }

  @Nested
  class MultipleTablesDeleteTest extends DynamoDbTestBase {
    private TransactionWriteMethod transaction;

    @Override
    @BeforeEach
    public void setUp() throws Exception {
      super.setUp();
      new TableProvisioner(getDbClient()).create(Table1.class);
      new TableProvisioner(getDbClient()).create(Table2.class);
      final Method method =
          TestDao.class.getMethod("delete2", String.class, String.class, int.class);
      final Signature signature = Signature.resolve(method, TestDao.class);
      transaction = new TransactionWriteMethod(getDbClient(), getDbMapper(), signature);
    }

    @Test
    void delete() throws Throwable {
      final String hashKey = "hashKey";
      final Table1 model1 = Table1.builder().hashKey(hashKey).integerAttribute(10).build();
      final Table2 model2 = Table2.builder().hashKey(hashKey).integerAttribute(10).build();
      getDbMapper().save(model1);
      getDbMapper().save(model2);

      transaction.invoke(hashKey, Table1.INT_ATTRIBUTE, 5);

      assertThat(getDbMapper().load(model1)).isNull();
      assertThat(getDbMapper().load(model2)).isNull();
    }
  }
}
