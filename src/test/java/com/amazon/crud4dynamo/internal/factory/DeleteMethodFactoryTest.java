package com.amazon.crud4dynamo.internal.factory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.amazon.crud4dynamo.annotation.Delete;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.internal.method.DeleteMethod;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import java.lang.reflect.Method;
import lombok.Data;
import org.junit.jupiter.api.Test;

class DeleteMethodFactoryTest extends SingleTableDynamoDbTestBase<DeleteMethodFactoryTest.Model> {
  @Data
  @DynamoDBTable(tableName = "Model")
  public static class Model {
    @DynamoDBHashKey(attributeName = "HashKey")
    private String hashKey;

    @DynamoDBAttribute(attributeName = "A")
    private Integer a;
  }

  @Override
  protected Class getModelClass() {
    return Model.class;
  }

  private interface Dao {
    void nonDeleteMethod();

    @Delete(keyExpression = "")
    void delete();
  }

  @Test
  void notGivenDeleteAnnotation_delegate() throws Exception {
    final AbstractMethodFactory delegate = mock(AbstractMethodFactory.class);
    final DeleteMethodFactory factory = new DeleteMethodFactory(delegate);
    final Context context = getContext("nonDeleteMethod");

    factory.create(context);

    verify(delegate).create(context);
  }

  @Test
  void givenDeleteAnnotation_createDeleteMethod() throws Exception {
    final AbstractMethodFactory delegate = mock(AbstractMethodFactory.class);
    final DeleteMethodFactory factory = new DeleteMethodFactory(delegate);
    final Context context = getContext("delete");

    final AbstractMethod method = factory.create(context);

    assertThat(method).isInstanceOf(DeleteMethod.class);
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

  private static Signature getSignature(final String name, final Class<?>... parameterTypes)
      throws NoSuchMethodException {
    final Method method = Dao.class.getMethod(name, parameterTypes);
    return Signature.resolve(method, Dao.class);
  }
}
