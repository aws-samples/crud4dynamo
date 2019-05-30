package com.amazon.crud4dynamo.internal.factory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import com.amazon.crud4dynamo.annotation.transaction.ConditionCheck;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.internal.method.transaction.TransactionWriteMethod;
import com.amazon.crud4dynamo.testbase.DynamoDbTestBase;
import com.amazon.crud4dynamo.testdata.DummyTable;
import org.junit.jupiter.api.Test;

class TransactionWriteMethodFactoryTest extends DynamoDbTestBase {
    private interface Dao {
        void nonTransactionWriteMethod();

        @ConditionCheck(tableClass = DummyTable.class, keyExpression = "", conditionExpression = "")
        void transactionWriteMethod();
    }

    @Test
    void withoutTransactionWriteAnnotation_callDelegate() throws Exception {
        final AbstractMethodFactory delegate = mock(AbstractMethodFactory.class);
        final TransactionWriteMethodFactory factory = new TransactionWriteMethodFactory(delegate);
        final Context context = getContext("nonTransactionWriteMethod");

        final AbstractMethod method = factory.create(context);

        assertThat(method).isNull();
        verify(delegate).create(context);
    }

    private Context getContext(final String methodName) throws NoSuchMethodException {
        final Signature signature = Signature.resolve(Dao.class.getMethod(methodName), Dao.class);
        return Context.builder().mapper(getDbMapper()).amazonDynamoDb(getDbClient()).signature(signature).build();
    }

    @Test
    void givenTransactionWriteAnnotation_createTransactionMethod() throws Exception {
        final AbstractMethodFactory delegate = mock(AbstractMethodFactory.class);
        final TransactionWriteMethodFactory factory = new TransactionWriteMethodFactory(delegate);
        final Context context = getContext("transactionWriteMethod");

        final AbstractMethod method = factory.create(context);

        assertThat(method).isInstanceOf(TransactionWriteMethod.class);
        verifyZeroInteractions(delegate);
    }
}
