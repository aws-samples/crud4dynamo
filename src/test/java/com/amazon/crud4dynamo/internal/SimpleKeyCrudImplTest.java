package com.amazon.crud4dynamo.internal;

import com.amazon.crud4dynamo.internal.SimpleKeyCrudImplTest.Model;
import com.amazon.crud4dynamo.testbase.SimpleKeyTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import java.util.Arrays;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class SimpleKeyCrudImplTest extends SimpleKeyTestBase<Model, SimpleKeyCrudImpl> {
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

    @Override
    @SuppressWarnings("unchecked")
    protected SimpleKeyCrudImpl newDao() {
        return new SimpleKeyCrudImpl<>(getDynamoDbMapper(), DynamoDBMapperConfig.DEFAULT, getModelClass());
    }

    @Override
    protected List<Model> getTestData() {
        return Arrays.asList(
                Model.builder().hashKey("A").integer1(1).build(),
                Model.builder().hashKey("B").integer1(2).build(),
                Model.builder().hashKey("C").integer1(3).build());
    }

    @Override
    protected Class getModelClass() {
        return Model.class;
    }
}
