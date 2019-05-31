package com.amazon.crud4dynamo.internal.factory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.amazon.crud4dynamo.annotation.Parallel;
import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.annotation.Scan;
import com.amazon.crud4dynamo.crudinterface.CompositeKeyCrud;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.internal.factory.ParallelScanMethodFactoryTest.Model;
import com.amazon.crud4dynamo.internal.method.scan.ParallelScanMethod;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import java.lang.reflect.Method;
import java.util.Iterator;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;

class ParallelScanMethodFactoryTest extends SingleTableDynamoDbTestBase<Model> {
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @DynamoDBTable(tableName = "Model")
  public static class Model {
    @DynamoDBHashKey(attributeName = "HashKey")
    private String hashKey;

    @DynamoDBRangeKey(attributeName = "RangeKey")
    private Integer rangeKey;
  }

  @Override
  protected Class getModelClass() {
    return Model.class;
  }

  public interface Dao extends CompositeKeyCrud<String, Integer, Model> {
    @Parallel(totalSegments = 10)
    @Scan(filter = "#rangeKey between :lower and :upper")
    Iterator<Model> scan(
        @Param("#rangeKey") String rangeKeyName,
        @Param(":lower") int lower,
        @Param(":upper") int upper);

    void aNormalMethod();
  }

  @Test
  void canCreateParallelScanMethod() throws Throwable {
    final AbstractMethodFactory mockDelegate = mock(AbstractMethodFactory.class);
    final ParallelScanMethodFactory factory = new ParallelScanMethodFactory(mockDelegate);
    final Method scan = Dao.class.getMethod("scan", String.class, int.class, int.class);

    final AbstractMethod abstractMethod = factory.create(createContext(scan));

    assertThat(abstractMethod).isInstanceOf(ParallelScanMethod.class);
  }

  private Context createContext(final Method method) throws NoSuchMethodException {
    return Context.builder()
        .signature(Signature.resolve(method, Dao.class))
        .modelType(getModelClass())
        .mapper(getDynamoDbMapper())
        .build();
  }

  @Test
  void withoutParallelAnnotation_invokeDelegate() throws Throwable {
    final AbstractMethodFactory mockDelegate = mock(AbstractMethodFactory.class);
    final ParallelScanMethodFactory factory = new ParallelScanMethodFactory(mockDelegate);
    final Method aNormalMethod = Dao.class.getMethod("aNormalMethod");
    final Context context = createContext(aNormalMethod);

    final AbstractMethod abstractMethod = factory.create(context);

    assertThat(abstractMethod).isNull();
    verify(mockDelegate).create(context);
  }
}
