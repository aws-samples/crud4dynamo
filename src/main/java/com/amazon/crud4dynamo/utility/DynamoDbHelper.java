package com.amazon.crud4dynamo.utility;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class DynamoDbHelper {
    public static String getTableName(final Class<?> tableType) {
        return tableType.getAnnotation(DynamoDBTable.class).tableName();
    }
}
