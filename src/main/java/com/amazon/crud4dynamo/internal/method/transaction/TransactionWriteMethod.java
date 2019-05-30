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

import com.amazon.crud4dynamo.annotation.transaction.ConditionCheck;
import com.amazon.crud4dynamo.annotation.transaction.Delete;
import com.amazon.crud4dynamo.annotation.transaction.Put;
import com.amazon.crud4dynamo.annotation.transaction.Update;
import com.amazon.crud4dynamo.extension.Argument;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItem;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsRequest;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TransactionWriteMethod implements AbstractMethod {
    private final AmazonDynamoDB amazonDynamoDB;
    private final Signature signature;
    private final List<ConditionCheckFactory> conditionalCheckFactories;
    private final List<DeleteFactory> deleteFactories;
    private final List<PutFactory> putFactories;
    private final List<UpdateFactory> updateFactories;

    public TransactionWriteMethod(final AmazonDynamoDB amazonDynamoDb, final DynamoDBMapper dynamoDbMapper, final Signature signature) {
        this.amazonDynamoDB = amazonDynamoDb;
        this.signature = signature;
        conditionalCheckFactories =
                signature
                        .getAnnotationsByType(ConditionCheck.class)
                        .stream()
                        .map(a -> new ConditionCheckFactory(a, dynamoDbMapper.getTableModel(a.tableClass())))
                        .collect(Collectors.toList());
        deleteFactories =
                signature
                        .getAnnotationsByType(Delete.class)
                        .stream()
                        .map(a -> new DeleteFactory(a, dynamoDbMapper.getTableModel(a.tableClass())))
                        .collect(Collectors.toList());
        putFactories =
                signature
                        .getAnnotationsByType(Put.class)
                        .stream()
                        .map(a -> new PutFactory(a, dynamoDbMapper.getTableModel(a.tableClass())))
                        .collect(Collectors.toList());
        updateFactories =
                signature
                        .getAnnotationsByType(Update.class)
                        .stream()
                        .map(a -> new UpdateFactory(a, dynamoDbMapper.getTableModel(a.tableClass())))
                        .collect(Collectors.toList());
    }

    @Override
    public Signature getSignature() {
        return signature;
    }

    @Override
    public Object invoke(final Object... args) throws Throwable {
        final List<Argument> arguments = Argument.newList(signature.parameters(), Arrays.asList(args));
        final TransactWriteItemsRequest request =
                new TransactWriteItemsRequest()
                        .withTransactItems(
                                Stream.of(getConditionChecks(arguments), getDeletes(arguments), getPuts(arguments), getUpdates(arguments))
                                        .flatMap(Function.identity())
                                        .collect(Collectors.toList()));
        amazonDynamoDB.transactWriteItems(request);
        return null;
    }

    private Stream<TransactWriteItem> getUpdates(final List<Argument> arguments) {
        return updateFactories.stream().map(f -> f.create(arguments)).map(u -> new TransactWriteItem().withUpdate(u));
    }

    private Stream<TransactWriteItem> getPuts(final List<Argument> arguments) {
        return putFactories.stream().map(f -> f.create(arguments)).map(p -> new TransactWriteItem().withPut(p));
    }

    private Stream<TransactWriteItem> getDeletes(final List<Argument> arguments) {
        return deleteFactories.stream().map(f -> f.create(arguments)).map(d -> new TransactWriteItem().withDelete(d));
    }

    private Stream<TransactWriteItem> getConditionChecks(final List<Argument> arguments) {
        return conditionalCheckFactories.stream().map(f -> f.create(arguments)).map(c -> new TransactWriteItem().withConditionCheck(c));
    }

    @Override
    public AbstractMethod bind(final Object target) {
        return this;
    }
}
