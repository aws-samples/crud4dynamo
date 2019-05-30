/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.crud4dynamo.internal.method.transaction;

import com.amazon.crud4dynamo.annotation.transaction.Get;
import com.amazon.crud4dynamo.exception.CrudForDynamoException;
import com.amazon.crud4dynamo.extension.Argument;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.utility.Reflection;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperTableModel;
import com.amazonaws.services.dynamodbv2.model.ItemResponse;
import com.amazonaws.services.dynamodbv2.model.TransactGetItem;
import com.amazonaws.services.dynamodbv2.model.TransactGetItemsRequest;
import com.amazonaws.services.dynamodbv2.model.TransactGetItemsResult;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import com.google.common.reflect.TypeToken;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class TransactionGetMethod implements AbstractMethod {
    private static final TypeToken<List<Object>> VALID_METHOD_RETURN_TYPE = new TypeToken<List<Object>>() {};
    private final AmazonDynamoDB dynamoDb;
    private final DynamoDBMapper dynamoDbMapper;
    private final Signature signature;
    private final List<GetFactory> getFactories;
    private final List<ItemResponseConverter> itemResponseConverters;

    public TransactionGetMethod(final AmazonDynamoDB dynamoDb, final DynamoDBMapper dynamoDbMapper, final Signature signature) {
        this.dynamoDb = dynamoDb;
        this.dynamoDbMapper = dynamoDbMapper;
        this.signature = checkSignatureOrThrow(signature);
        final List<Get> getAnnotations = signature.getAnnotationsByType(Get.class);
        getFactories =
                getAnnotations
                        .stream()
                        .map(a -> new GetFactory(a, dynamoDbMapper.getTableModel(a.tableClass())))
                        .collect(Collectors.toList());
        itemResponseConverters =
                getAnnotations.stream().map(a -> new ItemResponseConverter(dynamoDbMapper, a)).collect(Collectors.toList());
    }

    private static Signature checkSignatureOrThrow(final Signature signature) {
        if (!signature.returnType().equals(VALID_METHOD_RETURN_TYPE)) {
            throw new ReturnTypeInvalidException(signature.returnType());
        }
        return signature;
    }

    @Override
    public Signature getSignature() {
        return signature;
    }

    @Override
    public Object invoke(final Object... args) throws Throwable {
        final List<Argument> arguments = Argument.newList(signature.parameters(), Arrays.asList(args));
        final TransactGetItemsRequest request = new TransactGetItemsRequest().withTransactItems(newGetItems(arguments));
        final TransactGetItemsResult result = dynamoDb.transactGetItems(request);
        sanityCheck(result);
        return convertResult(result);
    }

    private List<TransactGetItem> newGetItems(final List<Argument> arguments) {
        return getFactories.stream().map(f -> f.create(arguments)).map(g -> new TransactGetItem().withGet(g)).collect(Collectors.toList());
    }

    private void sanityCheck(final TransactGetItemsResult result) {
        if (itemResponseConverters.size() != result.getResponses().size()) {
            throw new IllegalStateException(
                    String.format(
                            "Number of transaction get item responses (%d) does not equal to number of transact get item request (%d)",
                            result.getResponses().size(), itemResponseConverters.size()));
        }
    }

    private List<Object> convertResult(final TransactGetItemsResult result) {
        return Streams.zip(result.getResponses().stream(), itemResponseConverters.stream(), newConvertFunction())
                .collect(Collectors.toList());
    }

    private static BiFunction<ItemResponse, ItemResponseConverter, Object> newConvertFunction() {
        return (itemResponse, itemResponseConverter) -> itemResponseConverter.convert(itemResponse);
    }

    @Override
    public AbstractMethod bind(final Object target) {
        return this;
    }

    private static class ItemResponseConverter {
        private final DynamoDBMapper dynamoDbMapper;
        private final Get getAnnotation;
        private final DynamoDBMapperTableModel<?> tableModel;

        private ItemResponseConverter(final DynamoDBMapper dynamoDbMapper, final Get getAnnotation) {
            this.dynamoDbMapper = dynamoDbMapper;
            this.getAnnotation = getAnnotation;
            tableModel = dynamoDbMapper.getTableModel(getAnnotation.tableClass());
        }

        private Object convert(final ItemResponse itemResponse) {
            return Optional.ofNullable(itemResponse.getItem())
                    .map(item -> (Object) tableModel.unconvert(item))
                    .orElseGet(() -> Reflection.newInstance(getAnnotation.tableClass()));
        }
    }

    @VisibleForTesting
    static class ReturnTypeInvalidException extends CrudForDynamoException {
        private ReturnTypeInvalidException(final TypeToken<?> returnType) {
            super(
                    String.format(
                            "Expect the return type of method annotated with transaction @Get to be %s but is %s.",
                            VALID_METHOD_RETURN_TYPE, returnType));
        }
    }
}
