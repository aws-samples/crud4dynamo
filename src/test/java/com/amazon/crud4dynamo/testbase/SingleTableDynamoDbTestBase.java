package com.amazon.crud4dynamo.testbase;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperTableModel;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedScanList;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.local.shared.access.AmazonDynamoDBLocal;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class SingleTableDynamoDbTestBase<T> extends Log4jEnabledTestBase {
    private static final ProvisionedThroughput PROVISIONED_THROUGHPUT = new ProvisionedThroughput(5L, 5L);
    private static final Projection PROJECTION_ALL = new Projection().withProjectionType(ProjectionType.ALL);
    private AmazonDynamoDBLocal embeddedDynamoDb;
    private AmazonDynamoDB dynamoDbClient;
    private DynamoDBMapper dynamoDbMapper;
    private DynamoDBMapperTableModel tableModel;

    @BeforeEach
    public void setUp() throws Exception {
        AwsDynamoDbLocalTestUtils.initSqLite();

        embeddedDynamoDb = DynamoDBEmbedded.create();
        dynamoDbClient = embeddedDynamoDb.amazonDynamoDB();
        dynamoDbMapper = new DynamoDBMapper(dynamoDbClient);
        tableModel = dynamoDbMapper.getTableModel(getModelClass());
        createTable(getModelClass());
    }

    @AfterEach
    public void tearDown() throws Exception {
        embeddedDynamoDb.shutdown();
    }

    protected abstract Class<T> getModelClass();

    protected AmazonDynamoDBLocal getDynamoDbLocal() {
        return embeddedDynamoDb;
    }

    protected AmazonDynamoDB getDynamoDbClient() {
        return dynamoDbClient;
    }

    protected DynamoDBMapper getDynamoDbMapper() {
        return dynamoDbMapper;
    }

    protected DynamoDBMapperTableModel getDynamoDbMapperTableModel() {
        return tableModel;
    }

    protected void shutdownDb() {
        embeddedDynamoDb.shutdown();
    }

    protected Optional<Item> getItem(final GetItemSpec getItemSpec) {
        return Optional.ofNullable(new Table(dynamoDbClient, getTableName()).getItem(getItemSpec));
    }

    protected List<T> storeItems(final Stream<T> itemStream) {
        return itemStream.peek(dynamoDbMapper::save).collect(Collectors.toList());
    }

    protected List<T> storeItems(final T... items) {
        return storeItems(Stream.of(items));
    }

    protected List<T> getAllItems() {
        final PaginatedScanList<T> scan = dynamoDbMapper.scan(getModelClass(), new DynamoDBScanExpression());
        return Lists.newArrayList(scan.iterator());
    }

    protected Optional<T> getItem(final T item) {
        return Optional.ofNullable(dynamoDbMapper.load(item));
    }

    private String getTableName() {
        return getModelClass().getAnnotation(DynamoDBTable.class).tableName();
    }

    private void createTable(final Class<?> modelClass) throws Exception {
        dynamoDbClient.createTable(newCreateTableRequest(modelClass));
        waitTableToBeActive(modelClass);
    }

    private CreateTableRequest newCreateTableRequest(final Class<?> modelClass) {
        final CreateTableRequest createTableRequest = dynamoDbMapper.generateCreateTableRequest(modelClass);
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
        new Table(dynamoDbClient, modelClass.getAnnotation(DynamoDBTable.class).tableName()).waitForActive();
    }
}
