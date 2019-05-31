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

package com.amazon.crud4dynamo.internal.utility;

import com.amazon.crud4dynamo.exception.CrudForDynamoException;
import com.amazon.crud4dynamo.extension.Argument;
import com.amazon.crud4dynamo.internal.factory.ExpressionFactoryHelper;
import com.amazon.crud4dynamo.internal.parsing.AttributeValueMapper;
import com.amazon.crud4dynamo.internal.parsing.KeyExpressionMapper;
import com.amazon.crud4dynamo.utility.MapHelper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperTableModel;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class KeyAttributeConstructor {

  private final KeyExpressionMapper keyExpressionMapper;

  public KeyAttributeConstructor(
      final String keyExpression, final DynamoDBMapperTableModel<?> tableModel) {
    keyExpressionMapper = new KeyExpressionMapper(keyExpression, tableModel);
  }

  public Map<String, AttributeValue> create(final List<Argument> arguments) {
    final Map<String, String> expAttrNames =
        ExpressionFactoryHelper.getExpressionAttributeNames(arguments);
    final Map<String, AttributeValue> hashKey =
        getKey(keyExpressionMapper.getHashKeyContext(), arguments, expAttrNames);
    final Map<String, AttributeValue> rangeKey =
        keyExpressionMapper
            .getRangeKeyContext()
            .map(ctx -> getKey(ctx, arguments, expAttrNames))
            .orElse(Collections.emptyMap());
    return MapHelper.overrideMerge(hashKey, rangeKey);
  }

  private static Map<String, AttributeValue> getKey(
      final KeyExpressionMapper.Context context,
      final List<Argument> argList,
      final Map<String, String> expAttrNames) {
    final AttributeValueMapper valueMapper =
        context.getNameMapper().toValueMapper(expAttrNames).merge(context.getValueMapper());

    final Map<String, AttributeValue> exprAttrValues =
        ExpressionFactoryHelper.getExpressionAttributeValues(argList, valueMapper);
    final AttributeValue keyAttributeValue = getKeyAttributeOrThrow(context, exprAttrValues);
    final String keyName =
        Optional.ofNullable(expAttrNames.get(context.getKeyStringText()))
            .orElse(context.getKeyStringText());
    return ImmutableMap.of(keyName, keyAttributeValue);
  }

  private static AttributeValue getKeyAttributeOrThrow(
      KeyExpressionMapper.Context context, Map<String, AttributeValue> attrExpValues) {
    return attrExpValues.values().stream()
        .findFirst()
        .orElseThrow(() -> new NoKeyAttributeException(context.getKeyStringText()));
  }

  @VisibleForTesting
  static class NoKeyAttributeException extends CrudForDynamoException {
    NoKeyAttributeException(final String keyText) {
      super(String.format("There is no argument provided for key %s", keyText));
    }
  }
}
