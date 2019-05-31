package com.amazon.crud4dynamo.internal.method.transaction;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.annotation.transaction.ConditionCheck;
import com.amazon.crud4dynamo.annotation.transaction.Put;
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

public class TransactionTest extends DynamoDbTestBase {
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @DynamoDBTable(tableName = Customer.TABLE_NAME)
  public static class Customer {
    public static final String TABLE_NAME = "Customer";
    public static final String ID = "CustomerId";

    @DynamoDBHashKey(attributeName = ID)
    private String id;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @DynamoDBTable(tableName = ProductCatalog.TABLE_NAME)
  public static class ProductCatalog {
    public static final String TABLE_NAME = "ProductCatalog";
    public static final String ID = "ProductId";
    public static final String PRODUCT_STATUS = "ProductStatus";

    @DynamoDBHashKey(attributeName = ID)
    private String id;

    @DynamoDBAttribute(attributeName = PRODUCT_STATUS)
    private String status;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @DynamoDBTable(tableName = Orders.TABLE_NAME)
  public static class Orders {
    public static final String TABLE_NAME = "Orders";
    public static final String ID = "OrderId";
    public static final String ORDER_STATUS = "OrderStatus";
    public static final String TOTAL = "Total";

    @DynamoDBHashKey(attributeName = ID)
    private String id;

    @DynamoDBAttribute(attributeName = ORDER_STATUS)
    private String status;

    @DynamoDBAttribute(attributeName = TOTAL)
    private Integer total;
  }

  private interface Transaction {
    @ConditionCheck(
        tableClass = Customer.class,
        keyExpression = "CustomerId = :customerId",
        conditionExpression = "attribute_exists(CustomerId)")
    @Update(
        tableClass = ProductCatalog.class,
        keyExpression = "ProductId = :productId",
        updateExpression = "SET ProductStatus = :newProductStatus",
        conditionExpression = "ProductStatus = :expectedOldProductStatus")
    @Put(
        tableClass = Orders.class,
        item = ":newOrder",
        conditionExpression = "attribute_not_exists(OrderId)")
    void write(
        @Param(":customerId") final String customerId,
        @Param(":productId") final String productId,
        @Param(":newProductStatus") final String newProductStatus,
        @Param(":expectedOldProductStatus") final String expectedOldProductStatus,
        @Param(":newOrder") final Orders newOrder);
  }

  @Override
  @BeforeEach
  public void setUp() throws Exception {
    super.setUp();
    new TableProvisioner(getDbClient()).create(Customer.class);
    new TableProvisioner(getDbClient()).create(ProductCatalog.class);
    new TableProvisioner(getDbClient()).create(Orders.class);
  }

  @Nested
  class WriteTest {
    private final String customerId = "09e8e9c8-ec48";
    private final String productId = "Mac Book Pro";
    private final String expectedOldProductStatus = "IN_STOCK";
    private final String newProductStatus = "SOLD";
    private final Orders newOrder =
        Orders.builder().id("new order random id").status("CONFIRMED").total(100).build();

    private TransactionWriteMethod transaction;

    @BeforeEach
    void setUp() throws Exception {
      final Method method =
          Transaction.class.getMethod(
              "write", String.class, String.class, String.class, String.class, Orders.class);
      final Signature signature = Signature.resolve(method, Transaction.class);
      transaction = new TransactionWriteMethod(getDbClient(), getDbMapper(), signature);
    }

    @Test
    void writeSucceeded() throws Throwable {
      getDbMapper().save(Customer.builder().id(customerId).build());
      getDbMapper()
          .save(ProductCatalog.builder().id(productId).status(expectedOldProductStatus).build());

      transaction.invoke(
          customerId, productId, newProductStatus, expectedOldProductStatus, newOrder);

      assertThat(getDbMapper().load(ProductCatalog.builder().id(productId).build()).getStatus())
          .isEqualTo(newProductStatus);
      assertThat(getDbMapper().load(Orders.builder().id(newOrder.getId()).build()))
          .isEqualTo(newOrder);
    }
  }
}
