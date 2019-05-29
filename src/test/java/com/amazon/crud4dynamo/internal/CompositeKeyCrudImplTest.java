package com.amazon.crud4dynamo.internal;

import com.amazon.crud4dynamo.internal.CompositeKeyCrudImplTest.Model;
import com.amazon.crud4dynamo.testbase.CompositeKeyTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import java.util.Arrays;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class CompositeKeyCrudImplTest extends CompositeKeyTestBase<Model, CompositeKeyCrudImpl> {
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @DynamoDBTable(tableName = "TestTable")
    public static class Model {
        @DynamoDBHashKey(attributeName = "HashKey")
        private String hashKey;

        @DynamoDBRangeKey(attributeName = "RangeKey")
        private Integer rangeKey;

        @DynamoDBAttribute(attributeName = "StringAttribute")
        private String stringAttribute;

        @DynamoDBIndexRangeKey(localSecondaryIndexName = "Lsi", attributeName = "LsiRangeKey")
        private Integer lsiRangeKey;

        @DynamoDBIndexHashKey(globalSecondaryIndexName = "Gsi", attributeName = "GsiHashKey")
        private String gsiHashKey;

        @DynamoDBIndexRangeKey(globalSecondaryIndexName = "Gsi", attributeName = "GsiRangeKey")
        private Integer gsiRangeKey;
    }

    @Override
    protected Class<Model> getModelClass() {
        return Model.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected CompositeKeyCrudImpl newDao() {
        return new CompositeKeyCrudImpl<>(getDynamoDbMapper(), DynamoDBMapperConfig.DEFAULT, getModelClass());
    }

    @Override
    protected List<Model> getTestData() {
        return Arrays.asList(
                Model.builder().hashKey("A").rangeKey(1).stringAttribute("A").build(),
                Model.builder().hashKey("A").rangeKey(2).stringAttribute("B").build(),
                Model.builder().hashKey("A").rangeKey(3).stringAttribute("C").build(),
                Model.builder().hashKey("B").rangeKey(4).stringAttribute("D").build());
    }
}
