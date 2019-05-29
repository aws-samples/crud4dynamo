package com.amazon.crud4dynamo.internal;

import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ProxyTest {
    private interface Dao {
        void aMethod();
    }

    @Test
    void createProxy() throws Throwable {
        final AbstractMethod abstractMethod = mock(AbstractMethod.class);
        when(abstractMethod.bind(any())).thenReturn(abstractMethod);
        final Dao dao = new Proxy<>(Dao.class, method -> abstractMethod).create();

        dao.aMethod();

        verify(abstractMethod).bind(dao);
        verify(abstractMethod).invoke();
    }
}
