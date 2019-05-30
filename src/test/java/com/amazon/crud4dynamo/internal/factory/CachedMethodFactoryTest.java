package com.amazon.crud4dynamo.internal.factory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazon.crud4dynamo.annotation.Cached;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.internal.method.CachedMethod;
import java.lang.reflect.Method;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CachedMethodFactoryTest {

    private AbstractMethodFactory delegateFactory;
    private AbstractMethod delegateMethod;
    private CachedMethodFactory cachedMethodFactory;

    public interface TestInterface {
        @Cached
        void cachedMethod();

        void nonCachedMethod();
    }

    @BeforeEach
    void setUp() {
        delegateFactory = mock(AbstractMethodFactory.class);
        delegateMethod = mock(AbstractMethod.class);
        when(delegateFactory.create(any())).thenReturn(delegateMethod);
        cachedMethodFactory = new CachedMethodFactory(delegateFactory);
    }

    @Test
    void createCachedMethod() throws Exception {
        final Context context = getContext("cachedMethod");

        assertThat(cachedMethodFactory.create(context)).isInstanceOf(CachedMethod.class);
        verify(delegateFactory).create(context);
    }

    private Context getContext(final String methodName) throws NoSuchMethodException {
        final Method method = TestInterface.class.getMethod(methodName);
        return Context.builder()
                .interfaceType(TestInterface.class)
                .method(method)
                .signature(Signature.resolve(method, TestInterface.class))
                .build();
    }

    @Test
    void returnDelegateForNonCachedMethod() throws Exception {
        final Context context = getContext("nonCachedMethod");

        assertThat(cachedMethodFactory.create(context)).isEqualTo(delegateMethod);
        verify(delegateFactory).create(context);
    }
}
