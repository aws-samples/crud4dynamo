package com.amazon.crud4dynamo.internal.factory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.annotation.Put;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.internal.method.PutMethod;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import java.lang.reflect.Method;
import lombok.Data;
import org.junit.jupiter.api.Test;

class PutMethodFactoryTest extends SingleTableDynamoDbTestBase<PutMethodFactoryTest.Model> {
  private static Signature getSignature(final String name, final Class<?>... parameterTypes)
      throws NoSuchMethodException {
    final Method method = Dao.class.getMethod(name, parameterTypes);
    return Signature.resolve(method, Dao.class);
  }

  @Override
  protected Class<Model> getModelClass() {
    return Model.class;
  }

  @Test
  void notGivenPutAnnotation_delegate() throws Exception {
    final AbstractMethodFactory delegate = mock(AbstractMethodFactory.class);
    final PutMethodFactory putMethodFactory = new PutMethodFactory(delegate);
    final Context context = getContext("nonPutMethod");

    putMethodFactory.create(context);

    verify(delegate).create(context);
  }

  @Test
  void givenPutAnnotation_createPutMethod() throws Exception {
    final AbstractMethodFactory delegate = mock(AbstractMethodFactory.class);
    final PutMethodFactory putMethodFactory = new PutMethodFactory(delegate);
    final Context context = getContext("putMethod", Model.class);

    final AbstractMethod method = putMethodFactory.create(context);

    assertThat(method).isNotNull().isInstanceOf(PutMethod.class);
  }

  private Context getContext(final String methodName, final Class<?>... parameterTypes)
      throws NoSuchMethodException {
    return Context.builder()
        .signature(getSignature(methodName, parameterTypes))
        .mapper(getDynamoDbMapper())
        .amazonDynamoDb(getDynamoDbClient())
        .mapperConfig(DynamoDBMapperConfig.DEFAULT)
        .modelType(getModelClass())
        .build();
  }

  public interface Dao {
    void nonPutMethod();

    @Put
    void putMethod(final @Param(":item") Model model);
  }

  @Data
  @DynamoDBTable(tableName = "Model")
  public static class Model {
    @DynamoDBHashKey(attributeName = "HashKey")
    private String hashKey;

    @DynamoDBAttribute(attributeName = "A")
    private Integer a;
  }
}
