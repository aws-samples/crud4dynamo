package com.amazon.crud4dynamo.internal.method;

import com.amazon.crud4dynamo.annotation.Delete;
import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DeleteMethodTest extends SingleTableDynamoDbTestBase<DeleteMethodTest.Model> {
    private static final String KEY_EXPRESSION = "HashKey = :keyValue";

    @Builder
    @Data
    @DynamoDBTable(tableName = "Model")
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Model {
        @DynamoDBHashKey(attributeName = "HashKey")
        private String hashKey;

        @DynamoDBAttribute(attributeName = "A")
        private Integer a;
    }

    private interface Dao {
        @Delete(keyExpression = KEY_EXPRESSION)
        void deleteWithVoidReturnType(@Param(":keyValue") final String keyValue);

        @Delete(keyExpression = KEY_EXPRESSION, returnValue = ReturnValue.ALL_OLD)
        Model deleteWithOldValue(@Param(":keyValue") final String keyValue);
    }

    @Override
    protected Class<Model> getModelClass() {
        return Model.class;
    }

    @Test
    void delete_witNonReturnValue() throws Throwable {
        final DeleteMethod method = newMethod("deleteWithVoidReturnType");
        final String hashKey = "dummy";
        final Model dummyItem = Model.builder().hashKey(hashKey).build();
        storeItems(dummyItem);

        final Object result = method.invoke(hashKey);

        assertThat(result).isNull();
        assertThat(getItem(dummyItem)).isEmpty();
    }

    @Test
    void deleteReturnOldValue() throws Throwable {
        final DeleteMethod method = newMethod("deleteWithOldValue");
        final String hashKey = "dummy";
        final Model dummyItem = Model.builder().hashKey(hashKey).build();
        storeItems(dummyItem);

        final Object result = method.invoke(hashKey);

        assertThat(result).isEqualTo(dummyItem);
        assertThat(getItem(dummyItem)).isEmpty();
    }

    private DeleteMethod newMethod(final String methodName) throws NoSuchMethodException {
        final Signature defaultPutMethod = Signature.resolve(Dao.class.getMethod(methodName, String.class), Dao.class);
        return new DeleteMethod(defaultPutMethod, getModelClass(), getDynamoDbMapper(), getDynamoDbClient(), DynamoDBMapperConfig.DEFAULT);
    }
}
