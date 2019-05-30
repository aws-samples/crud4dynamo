package com.amazon.crud4dynamo.internal.factory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.amazon.crud4dynamo.crudinterface.CompositeKeyCrud;
import com.amazon.crud4dynamo.crudinterface.SimpleKeyCrud;
import com.amazon.crud4dynamo.exception.CrudForDynamoException;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.testbase.DynamoDbTestBase;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import java.lang.reflect.Method;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class BasicCrudMethodFactoryTest {

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @DynamoDBTable(tableName = "SimpleModelTable")
    public static class SimpleModel {
        @DynamoDBHashKey(attributeName = "HashKey")
        private String hashKey;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @DynamoDBTable(tableName = "CompositeModelTable")
    public static class CompositeModel {
        @DynamoDBHashKey(attributeName = "HashKey")
        private String hashKey;

        @DynamoDBRangeKey(attributeName = "RangeKey")
        private Integer rangeKey;
    }

    public interface CustomSimpleKeyCrud extends SimpleKeyCrud<String, SimpleModel> {
        @Override
        void save(final SimpleModel simpleModel) throws CrudForDynamoException;
    }

    @Nested
    class SimpleModelTest extends SingleTableDynamoDbTestBase<SimpleModel> {
        @Override
        protected Class<SimpleModel> getModelClass() {
            return SimpleModel.class;
        }

        @Test
        void canHandleMethodOfSimpleKeyCrud() throws Throwable {
            final BasicCrudMethodFactory factory = new BasicCrudMethodFactory(null);
            final Method method = SimpleKeyCrud.class.getMethod("save", Object.class);
            final SimpleModel model = SimpleModel.builder().hashKey("A").build();

            final AbstractMethod abstractMethod =
                    factory.create(newContext(method, SimpleKeyCrud.class, getModelClass(), getDynamoDbMapper()));

            assertThat(getItem(model)).isEmpty();
            abstractMethod.invoke(model);
            assertThat(getItem(model)).contains(model);
        }

        @Test
        void canHandleOverriddenMethod() throws Throwable {
            final BasicCrudMethodFactory factory = new BasicCrudMethodFactory(null);
            final Method method = CustomSimpleKeyCrud.class.getMethod("save", SimpleModel.class);
            final Context context = newContext(method, CustomSimpleKeyCrud.class, getModelClass(), getDynamoDbMapper());

            final SimpleModel model = SimpleModel.builder().hashKey("A").build();

            final AbstractMethod abstractMethod = factory.create(context);

            assertThat(getItem(model)).isEmpty();
            abstractMethod.invoke(model);
            assertThat(getItem(model)).contains(model);
        }
    }

    public interface CustomCompositeKeyCrud extends CompositeKeyCrud<String, Integer, CompositeModel> {
        @Override
        void save(final CompositeModel compositeModel) throws CrudForDynamoException;
    }

    @Nested
    class CompositeModelTest extends SingleTableDynamoDbTestBase<CompositeModel> {
        @Override
        protected Class<CompositeModel> getModelClass() {
            return CompositeModel.class;
        }

        @Test
        void canHandleMethodOfCompositeKeyCrud() throws Throwable {
            final BasicCrudMethodFactory factory = new BasicCrudMethodFactory(null);
            final Method method = CompositeKeyCrud.class.getMethod("save", Object.class);
            final CompositeModel model = CompositeModel.builder().hashKey("A").rangeKey(1).build();

            final AbstractMethod abstractMethod =
                    factory.create(newContext(method, CompositeKeyCrud.class, getModelClass(), getDynamoDbMapper()));

            assertThat(getItem(model)).isEmpty();
            abstractMethod.invoke(model);
            assertThat(getItem(model)).contains(model);
        }

        @Test
        void canHandleOverriddenMethod() throws Throwable {
            final BasicCrudMethodFactory factory = new BasicCrudMethodFactory(null);
            final Method method = CustomCompositeKeyCrud.class.getMethod("save", CompositeModel.class);
            final CompositeModel model = CompositeModel.builder().hashKey("A").rangeKey(1).build();

            final AbstractMethod abstractMethod =
                    factory.create(newContext(method, CustomCompositeKeyCrud.class, getModelClass(), getDynamoDbMapper()));

            assertThat(getItem(model)).isEmpty();
            abstractMethod.invoke(model);
            assertThat(getItem(model)).contains(model);
        }
    }

    public interface ExtendedInterface extends SimpleKeyCrud<String, SimpleModel> {
        void query();
    }

    @Nested
    class DelegationTest extends SingleTableDynamoDbTestBase<SimpleModel> {
        @Override
        protected Class<SimpleModel> getModelClass() {
            return SimpleModel.class;
        }

        @Test
        void delegateToSuper() throws Exception {
            final AbstractMethodFactory delegate = mock(AbstractMethodFactory.class);
            final Method method = ExtendedInterface.class.getMethod("query");
            final Context context = newContext(method, ExtendedInterface.class, SimpleModel.class, getDynamoDbMapper());

            final AbstractMethod abstractMethod = new BasicCrudMethodFactory(delegate).create(context);

            assertThat(abstractMethod).isNull();
            verify(delegate).create(context);
        }
    }

    @Nested
    class MultipleInterfaceMethodsCreationTest extends DynamoDbTestBase {

        @Test
        void createMethodForDifferentInterfaces_shouldNotThrowException() throws Throwable {
            final BasicCrudMethodFactory factory = new BasicCrudMethodFactory(new ThrowingMethodFactory(null));

            {
                final Method method = CustomSimpleKeyCrud.class.getMethod("save", SimpleModel.class);
                factory.create(newContext(method, CustomSimpleKeyCrud.class, SimpleModel.class, getDbMapper()));
            }

            {
                final Method method = CustomCompositeKeyCrud.class.getMethod("save", CompositeModel.class);
                factory.create(newContext(method, CustomCompositeKeyCrud.class, CompositeModel.class, getDbMapper()));
            }
        }
    }

    private static Context newContext(
            final Method method, final Class<?> interfaceType, final Class<?> modelType, final DynamoDBMapper mapper) {
        return Context.builder()
                .method(method)
                .mapper(mapper)
                .modelType(modelType)
                .interfaceType(interfaceType)
                .signature(Signature.resolve(method, interfaceType))
                .build();
    }
}
