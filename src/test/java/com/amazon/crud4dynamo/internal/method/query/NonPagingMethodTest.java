package com.amazon.crud4dynamo.internal.method.query;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.annotation.Query;
import com.amazon.crud4dynamo.crudinterface.CompositeKeyCrud;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.internal.method.query.NonPagingMethodTest.Model;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;

class NonPagingMethodTest extends SingleTableDynamoDbTestBase<Model> {

    private static final String KEY_CONDITION_EXPRESSION = "HashKey = :hashKey and RangeKey > :lower";

    private static final String GROUP_KEY = "A";

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @DynamoDBTable(tableName = "Model")
    public static class Model {
        @DynamoDBHashKey(attributeName = "HashKey")
        private String hashKey;

        @DynamoDBRangeKey(attributeName = "RangeKey")
        private Integer rangeKey;
    }

    @Override
    protected Class<Model> getModelClass() {
        return Model.class;
    }

    private interface Dao extends CompositeKeyCrud<String, Integer, Model> {
        @Query(keyCondition = KEY_CONDITION_EXPRESSION)
        Iterable<Model> query(@Param(":hashKey") final String hashKey, @Param(":lower") final int lower);
    }

    @Test
    void invoke() throws Throwable {
        final List<Model> testModels = prepareTestModels();
        final NonPagingMethod queryMethod = getQueryMethod();

        final Iterator<Model> models = (Iterator<Model>) queryMethod.invoke(GROUP_KEY, 1);

        assertThat(models).isNotNull();
        assertThat(Lists.newArrayList(models)).containsExactly(testModels.get(1));
    }

    private NonPagingMethod getQueryMethod() throws NoSuchMethodException {
        final Method method = Dao.class.getMethod("query", String.class, int.class);
        return new NonPagingMethod(Signature.resolve(method, Dao.class), getModelClass(), getDynamoDbMapper(), null);
    }

    private List<Model> prepareTestModels() {
        return storeItems(
                Stream.of(Model.builder().hashKey(GROUP_KEY).rangeKey(1).build(), Model.builder().hashKey(GROUP_KEY).rangeKey(2).build()));
    }
}
