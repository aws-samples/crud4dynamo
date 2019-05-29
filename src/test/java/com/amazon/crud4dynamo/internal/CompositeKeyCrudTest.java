package com.amazon.crud4dynamo.internal;

import com.amazon.crud4dynamo.CrudForDynamo;
import com.amazon.crud4dynamo.crudinterface.CompositeKeyCrud;
import com.amazon.crud4dynamo.internal.CompositeKeyCrudTest.Model;
import com.amazon.crud4dynamo.testbase.CompositeKeyTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import java.util.Arrays;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class CompositeKeyCrudTest extends CompositeKeyTestBase<Model, CompositeKeyCrud> {
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
    }

    @Override
    protected CompositeKeyCrud newDao() {
        return new CrudForDynamo(getDynamoDbClient()).createComposite(Model.class);
    }

    @Override
    protected List<Model> getTestData() {
        return Arrays.asList(
                Model.builder().hashKey("A").rangeKey(1).build(),
                Model.builder().hashKey("A").rangeKey(2).build(),
                Model.builder().hashKey("B").rangeKey(3).build());
    }

    @Override
    protected Class<Model> getModelClass() {
        return Model.class;
    }
}
