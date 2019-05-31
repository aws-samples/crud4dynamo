package com.amazon.crud4dynamo.testdata;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@DynamoDBTable(tableName = DummyTable.NAME)
public class DummyTable {
  public static final String NAME = "DummyTable";

  @DynamoDBHashKey(attributeName = "HashKey")
  public String hashKey;
}
