package com.amazon.crud4dynamo.testhelper;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import java.util.Optional;
import java.util.function.Consumer;

public class TableProvisioner {
    private static final ProvisionedThroughput PROVISIONED_THROUGHPUT = new ProvisionedThroughput(10L, 10L);
    private static final Projection PROJECTION_ALL = new Projection().withProjectionType(ProjectionType.ALL);
    private final AmazonDynamoDB dynamoDb;
    private final DynamoDBMapper dynamoDbMapper;

    public TableProvisioner(final AmazonDynamoDB dynamoDb) {
        this.dynamoDb = dynamoDb;
        dynamoDbMapper = new DynamoDBMapper(dynamoDb);
    }

    public void create(final Class<?> tableClass) throws Exception {
        dynamoDb.createTable(newCreateTableRequest(tableClass));
        waitTableToBeActive(tableClass);
    }

    private CreateTableRequest newCreateTableRequest(final Class<?> tableClass) {
        final CreateTableRequest createTableRequest = dynamoDbMapper.generateCreateTableRequest(tableClass);
        createTableRequest.setProvisionedThroughput(PROVISIONED_THROUGHPUT);
        Optional.ofNullable(createTableRequest.getGlobalSecondaryIndexes()).ifPresent(gsis -> gsis.forEach(configureGsi()));
        return createTableRequest;
    }

    private static Consumer<GlobalSecondaryIndex> configureGsi() {
        return gsi -> {
            gsi.setProvisionedThroughput(PROVISIONED_THROUGHPUT);
            gsi.setProjection(PROJECTION_ALL);
        };
    }

    private void waitTableToBeActive(final Class<?> modelClass) throws InterruptedException {
        new Table(dynamoDb, modelClass.getAnnotation(DynamoDBTable.class).tableName()).waitForActive();
    }
}
