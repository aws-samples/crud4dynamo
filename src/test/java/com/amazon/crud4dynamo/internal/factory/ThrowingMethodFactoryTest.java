package com.amazon.crud4dynamo.internal.factory;

import com.amazon.crud4dynamo.exception.CrudForDynamoException;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.Signature;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ThrowingMethodFactoryTest {
    @Test
    void throwException() throws Exception {
        final ThrowingMethodFactory throwingMethodFactory = new ThrowingMethodFactory(null);
        final Context context = prepareContext();

        assertThatThrownBy(() -> throwingMethodFactory.create(context)).isInstanceOf(CrudForDynamoException.class);
    }

    private Context prepareContext() throws NoSuchMethodException {
        final Method method = String.class.getMethod("length");
        final Signature signature = Signature.resolve(method, String.class);
        return Context.builder().interfaceType(String.class).signature(signature).method(method).build();
    }
}
