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

package com.amazon.crud4dynamo.internal.parsing;

import com.amazon.crud4dynamo.extension.Argument;
import com.amazon.crud4dynamo.internal.factory.ExpressionFactoryHelper;
import com.amazon.crud4dynamo.utility.MapHelper;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import lombok.NonNull;

public class ExpressionAttributesFactory {
    private final ExpressionParser mergedParser;

    public ExpressionAttributesFactory(final ExpressionParser... parsers) {
        mergedParser = Stream.of(parsers).reduce(ExpressionParser.newEmptyInstance(), ExpressionParser::merge);
    }

    public Map<String, String> newExpressionAttributeNames(@NonNull final List<Argument> arguments) {
        final Set<String> names = mergedParser.getExpressionAttributeNames();
        return MapHelper.toNullIfEmpty(ExpressionFactoryHelper.getExpressionAttributeNames(arguments, names::contains));
    }

    public Map<String, AttributeValue> newExpressionAttributeValues(@NonNull final List<Argument> arguments) {
        final AttributeValueMapper mergedMapper =
                mergedParser
                        .getAttributeNameMapper()
                        .toValueMapper(ExpressionFactoryHelper.getExpressionAttributeNames(arguments))
                        .merge(mergedParser.getAttributeValueMapper());
        return MapHelper.toNullIfEmpty(ExpressionFactoryHelper.getExpressionAttributeValues(arguments, mergedMapper));
    }
}
