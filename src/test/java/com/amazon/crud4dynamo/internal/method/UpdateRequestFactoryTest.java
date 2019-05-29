package com.amazon.crud4dynamo.internal.method;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.annotation.Update;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.internal.method.UpdateRequestFactoryTest.Model;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import java.lang.reflect.Method;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class UpdateRequestFactoryTest extends SingleTableDynamoDbTestBase<Model> {
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @DynamoDBTable(tableName = "Table")
    public static class Model {
        @DynamoDBHashKey(attributeName = "HashKey")
        private String hashKey;

        @DynamoDBRangeKey(attributeName = "RangeKey")
        private Integer rangeKey;

        @DynamoDBAttribute(attributeName = "Str1")
        private String str1;
    }

    private interface Dao {
        /* keyExpression shares nothing with other expressions. */
        @Update(keyExpression = "HashKey = :hashKey, #rangeKey = :rangeKey", updateExpression = "SET RangeKey = RangeKey")
        void update1(
                @Param(":hashKey") final String hashKey,
                @Param("#rangeKey") final String rangeKey,
                @Param(":rangeKey") final int rangeKeyValue);

        /* keyExpression shares a argument with other expressions. */
        @Update(keyExpression = "HashKey = :hashKey, #rangeKey = :rangeKey", updateExpression = "SET RangeKey = :rangeKey")
        void update2(
                @Param(":hashKey") final String hashKeyValue,
                @Param("#rangeKey") final String rangeKeyName,
                @Param(":rangeKey") final int rangeKeyValue);

        @Update(
                keyExpression = "HashKey = :hashKey, #rangeKey = :rangeKey",
                updateExpression = "SET RangeKey = :rangeKey",
                conditionExpression = "#attr in (:str)")
        void update3(
                @Param(":hashKey") final String hashKeyValue,
                @Param("#rangeKey") final String rangeKeyName,
                @Param(":rangeKey") final int rangeKeyValue,
                @Param("#attr") final String attrName,
                @Param(":str") final String str);

        @Update(
                keyExpression = "HashKey = :hashKey, #rangeKey = :rangeKey",
                updateExpression = "SET #attr = #attr",
                returnValue = ReturnValue.ALL_OLD)
        void update4(
                @Param(":hashKey") final String hashKeyValue,
                @Param("#rangeKey") final String rangeKeyName,
                @Param(":rangeKey") final int rangeKeyValue,
                @Param("#attr") final String attrName);
    }

    @Override
    protected Class<Model> getModelClass() {
        return Model.class;
    }

    @Test
    void keyExpression_Shares_Nothing_With_OtherExpressions() throws Throwable {
        final Method updateRangeKeyMethod = Dao.class.getMethod("update1", String.class, String.class, int.class);
        final Signature signature = Signature.resolve(updateRangeKeyMethod, Dao.class);
        final UpdateRequestFactory updateRequestFactory = new UpdateRequestFactory(signature, getModelClass(), getDynamoDbMapper());

        final UpdateItemRequest updateItemRequest = updateRequestFactory.create("hashKeyValue", "RangeKey", 1984);

        assertThat(updateItemRequest.getTableName()).isEqualTo("Table");
        assertThat(updateItemRequest.getKey()).containsEntry("HashKey", new AttributeValue("hashKeyValue"));
        assertThat(updateItemRequest.getKey()).containsEntry("RangeKey", new AttributeValue().withN("1984"));
        assertThat(updateItemRequest.getUpdateExpression()).isEqualTo("SET RangeKey = RangeKey");
        assertThat(updateItemRequest.getExpressionAttributeNames()).isNull();
        assertThat(updateItemRequest.getExpressionAttributeValues()).isNull();
        assertThat(updateItemRequest.getConditionExpression()).isNull();
    }

    @Test
    void keyExpression_Shares_A_Argument_With_OtherExpressions() throws Throwable {
        final Method updateRangeKeyMethod = Dao.class.getMethod("update2", String.class, String.class, int.class);
        final Signature signature = Signature.resolve(updateRangeKeyMethod, Dao.class);
        final UpdateRequestFactory updateRequestFactory = new UpdateRequestFactory(signature, getModelClass(), getDynamoDbMapper());

        final UpdateItemRequest updateItemRequest = updateRequestFactory.create("hashKeyValue", "RangeKey", 1984);

        assertThat(updateItemRequest.getTableName()).isEqualTo("Table");
        assertThat(updateItemRequest.getKey()).containsEntry("HashKey", new AttributeValue("hashKeyValue"));
        assertThat(updateItemRequest.getKey()).containsEntry("RangeKey", new AttributeValue().withN("1984"));
        assertThat(updateItemRequest.getUpdateExpression()).isEqualTo("SET RangeKey = :rangeKey");
        assertThat(updateItemRequest.getExpressionAttributeNames()).isNull();
        assertThat(updateItemRequest.getExpressionAttributeValues()).containsOnlyKeys(":rangeKey");
        assertThat(updateItemRequest.getExpressionAttributeValues()).containsValues(new AttributeValue().withN("1984"));
        assertThat(updateItemRequest.getConditionExpression()).isNull();
        assertThat(updateItemRequest.getReturnValues()).isEqualTo(ReturnValue.NONE.toString());
    }

    @Test
    void withConditionExpression() throws Throwable {
        final Method updateRangeKeyMethod =
                Dao.class.getMethod("update3", String.class, String.class, int.class, String.class, String.class);
        final Signature signature = Signature.resolve(updateRangeKeyMethod, Dao.class);
        final UpdateRequestFactory updateRequestFactory = new UpdateRequestFactory(signature, getModelClass(), getDynamoDbMapper());

        final UpdateItemRequest updateItemRequest = updateRequestFactory.create("hashKeyValue", "RangeKey", 1984, "Str1", "str");

        assertThat(updateItemRequest.getTableName()).isEqualTo("Table");
        assertThat(updateItemRequest.getKey()).containsEntry("HashKey", new AttributeValue("hashKeyValue"));
        assertThat(updateItemRequest.getKey()).containsEntry("RangeKey", new AttributeValue().withN("1984"));
        assertThat(updateItemRequest.getUpdateExpression()).isEqualTo("SET RangeKey = :rangeKey");
        assertThat(updateItemRequest.getConditionExpression()).isEqualTo("#attr in (:str)");
        assertThat(updateItemRequest.getExpressionAttributeNames()).containsEntry("#attr", "Str1");
        assertThat(updateItemRequest.getExpressionAttributeValues()).containsEntry(":rangeKey", new AttributeValue().withN("1984"));
        assertThat(updateItemRequest.getExpressionAttributeValues()).containsEntry(":str", new AttributeValue("str"));
        assertThat(updateItemRequest.getReturnValues()).isEqualTo(ReturnValue.NONE.toString());
    }

    @Test
    void updateExpression_Only_Contains_ExpressionAttributeNames() throws Throwable {
        final Method method = Dao.class.getMethod("update4", String.class, String.class, int.class, String.class);
        final Signature signature = Signature.resolve(method, Dao.class);
        final UpdateRequestFactory updateRequestFactory = new UpdateRequestFactory(signature, getModelClass(), getDynamoDbMapper());

        final UpdateItemRequest updateItemRequest = updateRequestFactory.create("hashKeyValue", "RangeKey", 1984, "Str1");

        assertThat(updateItemRequest.getTableName()).isEqualTo("Table");
        assertThat(updateItemRequest.getKey()).containsEntry("HashKey", new AttributeValue("hashKeyValue"));
        assertThat(updateItemRequest.getKey()).containsEntry("RangeKey", new AttributeValue().withN("1984"));
        assertThat(updateItemRequest.getUpdateExpression()).isEqualTo("SET #attr = #attr");
        assertThat(updateItemRequest.getExpressionAttributeNames()).containsEntry("#attr", "Str1");
        assertThat(updateItemRequest.getExpressionAttributeValues()).isNull();
        assertThat(updateItemRequest.getReturnValues()).isEqualTo(ReturnValue.ALL_OLD.toString());
    }
}
