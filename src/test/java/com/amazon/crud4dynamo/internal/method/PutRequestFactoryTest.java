package com.amazon.crud4dynamo.internal.method;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.annotation.Put;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Method;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

class PutRequestFactoryTest extends SingleTableDynamoDbTestBase<PutRequestFactoryTest.Model> {
    private static final String TABLE_NAME = "Table";

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @DynamoDBTable(tableName = TABLE_NAME)
    public static class Model {
        @DynamoDBHashKey(attributeName = "HashKey")
        private String hashKey;

        @DynamoDBRangeKey(attributeName = "RangeKey")
        private Integer rangeKey;

        @DynamoDBAttribute(attributeName = "Str1")
        private String str1;
    }

    @Override
    protected Class<Model> getModelClass() {
        return Model.class;
    }

    private interface Dao {
        void methodWithoutPutAnnotation();

        @Put
        void methodWithoutPutItem();

        @Put(returnValue = ReturnValue.ALL_NEW)
        void method_withNotNonReturnValue(@Param(":item") final Model item);

        @Put
        void defaultPut(@Param(":item") final Model item);

        @Put(returnValue = ReturnValue.ALL_OLD, conditionExpression = "#attribute > :value")
        Model method_withCustomPut(@Param(":item") final Model item, @Param("#attribute") String attribute, @Param(":value") String value);
    }

    @Test
    void methodWithoutPutAnnotation_throwException() throws Exception {
        assertThatThrownBy(() -> newFactory(Dao.class.getMethod("methodWithoutPutAnnotation")))
                .isInstanceOf(PutRequestFactory.NoPutAnnotationException.class);
    }

    @Test
    void methodWithoutPutItem_throwException() throws Exception {
        assertThatThrownBy(() -> newFactory(Dao.class.getMethod("methodWithoutPutItem")))
                .isInstanceOf(PutRequestFactory.NoPutItemAnnotationException.class);
    }

    @Test
    void method_withNonNonReturnValue_and_voidReturnType_throwException() throws Exception {
        assertThatThrownBy(() -> newFactory(Dao.class.getMethod("method_withNotNonReturnValue", Model.class)))
                .isInstanceOf(PutRequestFactory.ReturnTypeInvalidException.class);
    }

    @Test
    void method_withDefaultPutAnnotation() throws Exception {
        final PutRequestFactory factory = newFactory(Dao.class.getMethod("defaultPut", Model.class));
        final Model model = Model.builder().hashKey("hashKey").rangeKey(1).build();

        final PutItemRequest request = factory.create(model);

        assertThat(request)
                .isEqualTo(
                        new PutItemRequest()
                                .withTableName(TABLE_NAME)
                                .withItem(getDynamoDbMapperTableModel().convert(model))
                                .withExpressionAttributeNames(null)
                                .withExpressionAttributeValues(null)
                                .withReturnValues(ReturnValue.NONE));
    }

    @Test
    void method_withCustomPut() throws Exception {
        final PutRequestFactory factory = newFactory(Dao.class.getMethod("method_withCustomPut", Model.class, String.class, String.class));
        final Model model = Model.builder().hashKey("hashKey").rangeKey(1).build();
        final String attributeName = "Str1";
        final String dummyAttributeValue = "dummy";
        final PutItemRequest request = factory.create(model, attributeName, dummyAttributeValue);

        assertThat(request)
                .isEqualTo(
                        new PutItemRequest()
                                .withTableName(TABLE_NAME)
                                .withItem(getDynamoDbMapperTableModel().convert(model))
                                .withConditionExpression("#attribute > :value")
                                .withExpressionAttributeNames(ImmutableMap.of("#attribute", attributeName))
                                .withExpressionAttributeValues(ImmutableMap.of(":value", new AttributeValue(dummyAttributeValue)))
                                .withReturnValues(ReturnValue.ALL_OLD));
    }

    private PutRequestFactory newFactory(final Method method) {
        return new PutRequestFactory(Signature.resolve(method, Dao.class), getModelClass(), getDynamoDbMapper());
    }
}
