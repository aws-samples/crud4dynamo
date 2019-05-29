package com.amazon.crud4dynamo.internal.factory;

import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.internal.factory.DefaultMethodFactoryTest.Model;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class DefaultMethodFactoryTest extends SingleTableDynamoDbTestBase<Model> {

    private static final String DUMMY_STRING = "DUMMY STRING";

    public interface TestInterface {

        default String defaultMethod() {
            return DUMMY_STRING;
        }
    }

    private static final TestInterface IMPL = new TestInterface() {};

    private final AbstractMethodFactory dummyFactory = mock(AbstractMethodFactory.class);

    private Context context;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @DynamoDBTable(tableName = "TestTable")
    public static class Model {
        @DynamoDBHashKey(attributeName = "HashKey")
        private String hashKey;

        @DynamoDBAttribute(attributeName = "Integer1")
        private Integer integer1;
    }

    @BeforeEach
    @Override
    public void setUp() throws Exception {
        super.setUp();
        context =
                Context.builder()
                        .interfaceType(TestInterface.class)
                        .modelType(Model.class)
                        .mapper(getDynamoDbMapper())
                        .method(TestInterface.class.getMethod("defaultMethod"))
                        .build();
    }

    @Override
    protected Class<Model> getModelClass() {
        return Model.class;
    }

    @Test
    public void defaultMethod() throws Throwable {
        final AbstractMethod abstractMethod = new DefaultMethodFactory(dummyFactory).create(context);

        assertThat(abstractMethod.bind(IMPL).invoke()).isEqualTo(DUMMY_STRING);
    }
}
