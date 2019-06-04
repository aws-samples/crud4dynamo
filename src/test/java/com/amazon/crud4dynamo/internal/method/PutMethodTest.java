package com.amazon.crud4dynamo.internal.method;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.annotation.Put;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;

class PutMethodTest extends SingleTableDynamoDbTestBase<PutMethodTest.Model> {
  @Override
  protected Class<Model> getModelClass() {
    return Model.class;
  }

  @Test
  void defaultPutMethod() throws Throwable {
    final PutMethod putMethod = newMethod("defaultPutMethod");
    final String key = "A";
    final Model putItem = Model.builder().hashKey(key).a(1).build();

    final Object result = putMethod.invoke(putItem);

    assertThat(result).isNull();
    assertThat(getItem(Model.builder().hashKey(key).build())).contains(putItem);
  }

  private PutMethod newMethod(final String defaultPutMethod2) throws NoSuchMethodException {
    final Signature defaultPutMethod =
        Signature.resolve(Dao.class.getMethod(defaultPutMethod2, Model.class), Dao.class);
    return new PutMethod(
        defaultPutMethod,
        getModelClass(),
        getDynamoDbMapper(),
        getDynamoDbClient(),
        DynamoDBMapperConfig.DEFAULT);
  }

  @Test
  void putWithOldValueReturned() throws Throwable {
    final PutMethod putMethod = newMethod("putWithOldValueReturned");
    final String key = "A";
    final Model putItem1 = Model.builder().hashKey(key).a(1).build();
    final Model putItem2 = Model.builder().hashKey(key).a(2).build();

    {
      final Object result = putMethod.invoke(putItem1);
      assertThat(result).isNull();
      assertThat(getItem(Model.builder().hashKey(key).build())).contains(putItem1);
    }

    {
      final Object result = putMethod.invoke(putItem2);
      assertThat(result).isNotNull().isEqualTo(putItem1);
      assertThat(getItem(Model.builder().hashKey(key).build())).contains(putItem2);
    }
  }

  private interface Dao {
    @Put()
    void defaultPutMethod(final @Param(":item") Model model);

    @Put(returnValue = ReturnValue.ALL_OLD)
    Model putWithOldValueReturned(final @Param(":item") Model model);
  }

  @Builder
  @Data
  @DynamoDBTable(tableName = "Model")
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Model {
    @DynamoDBHashKey(attributeName = "HashKey")
    private String hashKey;

    @DynamoDBAttribute(attributeName = "A")
    private Integer a;
  }
}
