package com.amazon.crud4dynamo.testbase;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperTableModel;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.local.shared.access.AmazonDynamoDBLocal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class DynamoDbTestBase extends Log4jEnabledTestBase {
  private AmazonDynamoDBLocal embeddedDynamoDb;
  private AmazonDynamoDB dynamoDbClient;
  private DynamoDBMapper dynamoDbMapper;

  @BeforeEach
  public void setUp() throws Exception {
    AwsDynamoDbLocalTestUtils.initSqLite();

    embeddedDynamoDb = DynamoDBEmbedded.create();
    dynamoDbClient = embeddedDynamoDb.amazonDynamoDB();
    dynamoDbMapper = new DynamoDBMapper(dynamoDbClient);
  }

  @AfterEach
  public void tearDown() throws Exception {
    embeddedDynamoDb.shutdown();
  }

  protected AmazonDynamoDB getDbClient() {
    return dynamoDbClient;
  }

  protected DynamoDBMapper getDbMapper() {
    return dynamoDbMapper;
  }

  protected <T> DynamoDBMapperTableModel<T> getTableModel(final Class<T> modelClass) {
    return getDbMapper().getTableModel(modelClass);
  }
}
