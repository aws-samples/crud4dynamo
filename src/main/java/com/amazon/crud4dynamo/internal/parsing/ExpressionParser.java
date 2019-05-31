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

import com.google.common.collect.ImmutableSet;
import java.util.HashSet;
import java.util.Set;
import lombok.NonNull;

public interface ExpressionParser {
  AttributeNameMapper getAttributeNameMapper();

  AttributeValueMapper getAttributeValueMapper();

  Set<String> getExpressionAttributeNames();

  default ExpressionParser merge(@NonNull final ExpressionParser other) {
    return new ExpressionParser() {
      @Override
      public AttributeNameMapper getAttributeNameMapper() {
        return ExpressionParser.this.getAttributeNameMapper().merge(other.getAttributeNameMapper());
      }

      @Override
      public AttributeValueMapper getAttributeValueMapper() {
        return ExpressionParser.this
            .getAttributeValueMapper()
            .merge(other.getAttributeValueMapper());
      }

      @Override
      public Set<String> getExpressionAttributeNames() {
        final Set<String> names = new HashSet<>();
        names.addAll(ExpressionParser.this.getExpressionAttributeNames());
        names.addAll(other.getExpressionAttributeNames());
        return names;
      }
    };
  }

  static ExpressionParser newEmptyInstance() {
    return new ExpressionParser() {
      @Override
      public AttributeNameMapper getAttributeNameMapper() {
        return new AttributeNameMapper();
      }

      @Override
      public AttributeValueMapper getAttributeValueMapper() {
        return new AttributeValueMapper();
      }

      @Override
      public Set<String> getExpressionAttributeNames() {
        return ImmutableSet.of();
      }
    };
  }
}
