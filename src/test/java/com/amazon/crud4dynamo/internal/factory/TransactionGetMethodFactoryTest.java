package com.amazon.crud4dynamo.internal.factory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import com.amazon.crud4dynamo.annotation.transaction.Get;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.internal.method.transaction.TransactionGetMethod;
import com.amazon.crud4dynamo.testbase.DynamoDbTestBase;
import com.amazon.crud4dynamo.testdata.DummyTable;
import java.util.List;
import org.junit.jupiter.api.Test;

class TransactionGetMethodFactoryTest extends DynamoDbTestBase {
  @Test
  void withoutTransactionGetAnnotation_callDelegate() throws Exception {
    final AbstractMethodFactory delegate = mock(AbstractMethodFactory.class);
    final TransactionGetMethodFactory factory = new TransactionGetMethodFactory(delegate);
    final Context context = getContext("nonTransactionGet");

    final AbstractMethod method = factory.create(context);

    assertThat(method).isNull();
    verify(delegate).create(context);
  }

  private Context getContext(final String methodName) throws NoSuchMethodException {
    final Signature signature = Signature.resolve(Dao.class.getMethod(methodName), Dao.class);
    return Context.builder()
        .mapper(getDbMapper())
        .amazonDynamoDb(getDbClient())
        .signature(signature)
        .build();
  }

  @Test
  void givenTransactionGetAnnotation_createTransactionMethod() throws Exception {
    final AbstractMethodFactory delegate = mock(AbstractMethodFactory.class);
    final TransactionGetMethodFactory factory = new TransactionGetMethodFactory(delegate);
    final Context context = getContext("transactionGet");

    final AbstractMethod method = factory.create(context);

    assertThat(method).isInstanceOf(TransactionGetMethod.class);
    verifyZeroInteractions(delegate);
  }

  private interface Dao {
    void nonTransactionGet();

    @Get(tableClass = DummyTable.class, keyExpression = "", projectionExpression = "")
    List<Object> transactionGet();
  }
}
