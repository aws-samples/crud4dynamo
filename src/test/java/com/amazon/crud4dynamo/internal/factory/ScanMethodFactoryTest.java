package com.amazon.crud4dynamo.internal.factory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.annotation.Scan;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.PageRequest;
import com.amazon.crud4dynamo.extension.PageResult;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.internal.factory.ScanMethodFactoryTest.Model;
import com.amazon.crud4dynamo.internal.method.scan.NonPagingMethod;
import com.amazon.crud4dynamo.internal.method.scan.PagingMethod;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import java.lang.reflect.Method;
import java.util.Iterator;
import lombok.Data;
import org.junit.jupiter.api.Test;

class ScanMethodFactoryTest extends SingleTableDynamoDbTestBase<Model> {
  @Data
  @DynamoDBTable(tableName = "Model")
  public static class Model {
    @DynamoDBHashKey(attributeName = "HashKey")
    private String hashKey;
  }

  public interface TestInterface {
    void nonScan();

    @Scan(filter = "HashKey <> :hashKey")
    Iterator<Model> scan(@Param(":hashKey") final String keyValue);

    @Scan(filter = "HashKey <> :hashKey")
    PageResult<Model> pageScan(
        @Param(":hashKey") final String keyValue, final PageRequest<Model> request);
  }

  @Override
  protected Class<Model> getModelClass() {
    return Model.class;
  }

  @Test
  void notGivenScanAnnotation_delegate() throws NoSuchMethodException {
    final AbstractMethodFactory delegate = mock(AbstractMethodFactory.class);
    final ScanMethodFactory scanMethodFactory = new ScanMethodFactory(delegate);
    final Context context = getContext(TestInterface.class.getMethod("nonScan"));

    final AbstractMethod abstractMethod = scanMethodFactory.create(context);

    assertThat(abstractMethod).isNull();
    verify(delegate).create(context);
  }

  private Context getContext(final Method rawMethod) {
    return Context.builder()
        .signature(Signature.resolve(rawMethod, TestInterface.class))
        .interfaceType(TestInterface.class)
        .modelType(Model.class)
        .mapper(getDynamoDbMapper())
        .mapperConfig(null)
        .method(rawMethod)
        .build();
  }

  @Test
  void nonPagingScan() throws NoSuchMethodException {
    final Context context = getContext(TestInterface.class.getMethod("scan", String.class));
    final ScanMethodFactory scanMethodFactory = new ScanMethodFactory(null);

    final AbstractMethod abstractMethod = scanMethodFactory.create(context);

    assertThat(abstractMethod).isNotNull();
    assertThat(abstractMethod).isInstanceOf(NonPagingMethod.class);
  }

  @Test
  void pageScan() throws NoSuchMethodException {
    final Context context =
        getContext(TestInterface.class.getMethod("pageScan", String.class, PageRequest.class));
    final ScanMethodFactory scanMethodFactory = new ScanMethodFactory(null);

    final AbstractMethod abstractMethod = scanMethodFactory.create(context);

    assertThat(abstractMethod).isNotNull();
    assertThat(abstractMethod).isInstanceOf(PagingMethod.class);
  }
}
