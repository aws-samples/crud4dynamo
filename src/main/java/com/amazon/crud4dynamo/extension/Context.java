package com.amazon.crud4dynamo.extension;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import java.lang.reflect.Method;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

@Builder
@Value
@Accessors(fluent = true, chain = true)
public class Context {
    private final Signature signature;
    private final DynamoDBMapper mapper;
    private final AmazonDynamoDB amazonDynamoDb;
    private final Class<?> modelType;
    private final Class<?> interfaceType;
    private final DynamoDBMapperConfig mapperConfig;
    private final Method method;
}
