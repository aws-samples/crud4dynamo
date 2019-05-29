package com.amazon.crud4dynamo.internal.method;

import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperTableModel;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import java.util.Map;
import lombok.NonNull;

public class UpdateMethod implements AbstractMethod {

    private final Signature signature;
    private final Class<?> tableType;
    private final DynamoDBMapper mapper;
    private final AmazonDynamoDB dynamoDb;
    private final DynamoDBMapperConfig mapperConfig;
    private final UpdateRequestFactory updateRequestFactory;
    private final DynamoDBMapperTableModel<?> tableModel;

    public UpdateMethod(
            @NonNull final Signature signature,
            @NonNull final Class<?> tableType,
            @NonNull final DynamoDBMapper mapper,
            @NonNull final AmazonDynamoDB dynamoDb,
            final DynamoDBMapperConfig mapperConfig) {
        this.signature = signature;
        this.tableType = tableType;
        this.mapper = mapper;
        this.dynamoDb = dynamoDb;
        this.mapperConfig = mapperConfig;
        tableModel = mapper.getTableModel(tableType);
        updateRequestFactory = new UpdateRequestFactory(signature, tableType, mapper);
    }

    @Override
    public Signature getSignature() {
        return signature;
    }

    @Override
    public Object invoke(final Object... args) throws Throwable {
        final UpdateItemRequest updateItemRequest = updateRequestFactory.create(args);
        final UpdateItemResult updateItemResult = dynamoDb.updateItem(updateItemRequest);
        final Map<String, AttributeValue> attributes = updateItemResult.getAttributes();
        return attributes == null ? null : tableModel.unconvert(updateItemResult.getAttributes());
    }

    @Override
    public AbstractMethod bind(final Object target) {
        return this;
    }
}
