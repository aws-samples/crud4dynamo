package com.amazon.crud4dynamo.internal.factory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.amazon.crud4dynamo.annotation.Query;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.PageResult;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.internal.method.query.NonPagingMethod;
import com.amazon.crud4dynamo.internal.method.query.PagingMethod;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import java.lang.reflect.Method;
import lombok.Data;
import org.junit.jupiter.api.Test;

public class QueryMethodFactoryTest
    extends SingleTableDynamoDbTestBase<QueryMethodFactoryTest.TestModel> {

  @Data
  @DynamoDBTable(tableName = "Model")
  public static class TestModel {
    @DynamoDBHashKey(attributeName = "HashKey")
    private String hashKey;
  }

  public interface TestInterface {
    void nonQuery();

    @Query(keyCondition = "HashKey = :hashKey")
    void query();

    @Query(keyCondition = "HashKey = :hashKey")
    PageResult pagingQuery();
  }

  @Override
  protected Class<TestModel> getModelClass() {
    return TestModel.class;
  }

  @Test
  void notGivenQueryAnnotation_delegate() throws Exception {
    final Context context = getContext("nonQuery");
    final AbstractMethodFactory delegate = mock(AbstractMethodFactory.class);

    new QueryMethodFactory(delegate).create(context);

    verify(delegate).create(context);
  }

  private static Signature getSignature(final String nonQuery2) throws NoSuchMethodException {
    final Method nonQuery = TestInterface.class.getMethod(nonQuery2);
    return Signature.resolve(nonQuery, TestInterface.class);
  }

  @Test
  void nonPagingQuery() throws Exception {
    final Context context = getContext("query");

    assertThat(new QueryMethodFactory(null).create(context))
        .isExactlyInstanceOf(NonPagingMethod.class);
  }

  @Test
  void pagingQuery() throws Exception {
    final Context context = getContext("pagingQuery");

    assertThat(new QueryMethodFactory(null).create(context))
        .isExactlyInstanceOf(PagingMethod.class);
  }

  private Context getContext(final String name) throws NoSuchMethodException {
    return Context.builder()
        .signature(getSignature(name))
        .mapper(getDynamoDbMapper())
        .modelType(getModelClass())
        .build();
  }
}
