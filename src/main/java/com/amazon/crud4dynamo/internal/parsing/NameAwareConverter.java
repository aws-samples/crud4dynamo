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

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperTableModel;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import java.util.function.Function;

/** An attribute value converter which is aware of the expression attribute name, e.g, ":value" */
public interface NameAwareConverter extends AttributeValueConverter {
  String getName();

  static Function<String, NameAwareConverter> newLazyConverter(
      final String attributeValue, final DynamoDBMapperTableModel tableModel) {
    return name ->
        new NameAwareConverter() {
          @Override
          public String getName() {
            return attributeValue;
          }

          @Override
          public AttributeValue convert(final Object object) {
            return tableModel.field(name).convert(object);
          }
        };
  }
}
