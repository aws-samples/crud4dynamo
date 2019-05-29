package com.amazon.crud4dynamo.internal.factory;

import com.amazon.crud4dynamo.annotation.Cached;
import com.amazon.crud4dynamo.annotation.Custom;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CustomMethodFactoryTest {
    public interface TestInterface {
        @Custom(factoryClass = TestFactory.class)
        void aMethod();

        @Cached
        void bMethod();
    }

    public static class TestFactory implements AbstractMethodFactory {

        @Override
        public AbstractMethod create(final Context context) {
            final AbstractMethod mock = mock(AbstractMethod.class);
            when(mock.getSignature()).thenReturn(context.signature());
            return mock;
        }
    }

    @Test
    void createCustomMethod() throws Exception {
        final Method aMethod = TestInterface.class.getMethod("aMethod");
        final AbstractMethodFactory dummyDelegate = null;
        final Context context = Context.builder().signature(Signature.resolve(aMethod, TestInterface.class)).build();
        final CustomMethodFactory customMethodFactory = new CustomMethodFactory(dummyDelegate);

        final AbstractMethod abstractMethod = customMethodFactory.create(context);

        assertThat(abstractMethod).isNotNull();
        assertThat(abstractMethod.getSignature()).isEqualTo(context.signature());
    }

    @Test
    void noCreatedFromAnnotation_delegateToSuper() throws Exception {
        final Method bMethod = TestInterface.class.getMethod("bMethod");
        final AbstractMethodFactory mockDelegate = mock(AbstractMethodFactory.class);
        final Context context = Context.builder().signature(Signature.resolve(bMethod, TestInterface.class)).build();
        final CustomMethodFactory customMethodFactory = new CustomMethodFactory(mockDelegate);

        final AbstractMethod abstractMethod = customMethodFactory.create(context);

        assertThat(abstractMethod).isNull();
        verify(mockDelegate).create(context);
    }
}
