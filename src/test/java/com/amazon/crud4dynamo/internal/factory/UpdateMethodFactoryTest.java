package com.amazon.crud4dynamo.internal.factory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.amazon.crud4dynamo.annotation.Update;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.internal.factory.UpdateMethodFactoryTest.Model;
import com.amazon.crud4dynamo.internal.method.UpdateMethod;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import java.lang.reflect.Method;
import lombok.Data;
import org.junit.jupiter.api.Test;

class UpdateMethodFactoryTest extends SingleTableDynamoDbTestBase<Model> {

    @Data
    @DynamoDBTable(tableName = "Model")
    public static class Model {
        @DynamoDBHashKey(attributeName = "HashKey")
        private String hashKey;

        @DynamoDBAttribute(attributeName = "A")
        private Integer a;
    }

    public interface Dao {
        void nonUpdateMethod();

        @Update(keyExpression = "HashKey = :hashKey", updateExpression = "SET A = :b")
        void updateMethod();
    }

    @Override
    protected Class<Model> getModelClass() {
        return Model.class;
    }

    @Test
    void noGivenUpdateAnnotation_delegate() throws Throwable {
        final AbstractMethodFactory delegate = mock(AbstractMethodFactory.class);
        final UpdateMethodFactory updateMethodFactory = new UpdateMethodFactory(delegate);
        final Context context = getContext("nonUpdateMethod");

        final AbstractMethod method = updateMethodFactory.create(context);

        assertThat(method).isNull();
        verify(delegate).create(context);
    }

    @Test
    void givenUpdateAnnotation_createUpdateMethod() throws Throwable {
        final AbstractMethodFactory delegate = mock(AbstractMethodFactory.class);
        final UpdateMethodFactory updateMethodFactory = new UpdateMethodFactory(delegate);
        final Context context = getContext("updateMethod");

        final AbstractMethod method = updateMethodFactory.create(context);

        assertThat(method).isInstanceOf(UpdateMethod.class);
    }

    private Context getContext(final String methodName) throws NoSuchMethodException {
        return Context.builder()
                .signature(getSignature(methodName))
                .mapper(getDynamoDbMapper())
                .amazonDynamoDb(getDynamoDbClient())
                .mapperConfig(null)
                .modelType(getModelClass())
                .build();
    }

    private static Signature getSignature(final String name) throws NoSuchMethodException {
        final Method method = Dao.class.getMethod(name);
        return Signature.resolve(method, Dao.class);
    }
}
