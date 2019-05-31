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

import com.amazon.crud4dynamo.utility.MapHelper;
import java.util.HashMap;
import java.util.Map;
import lombok.EqualsAndHashCode;

/**
 * AttributeValueMapper internally holds a map
 *
 * <p>its key is the expression attribute value, e.g, ":value".
 *
 * <p>its value is a attribute value converter, when applied it converts the argument to its
 * corresponding DynamoDB attribute type.
 */
@EqualsAndHashCode
public class AttributeValueMapper {
  private final Map<String, AttributeValueConverter> innerMap;

  public AttributeValueMapper() {
    this(new HashMap<>());
  }

  public AttributeValueMapper(final Map<String, AttributeValueConverter> innerMap) {
    this.innerMap = innerMap;
  }

  public AttributeValueMapper merge(final AttributeValueMapper other) {
    return merge(this, other);
  }

  public static AttributeValueMapper merge(
      final AttributeValueMapper m1, final AttributeValueMapper m2) {
    return new AttributeValueMapper(MapHelper.overrideMerge(m1.innerMap, m2.innerMap));
  }

  public AttributeValueMapper put(final String name, final AttributeValueConverter converter) {
    innerMap.put(name, converter);
    return this;
  }

  public AttributeValueConverter get(final String name) {
    return innerMap.get(name);
  }
}
