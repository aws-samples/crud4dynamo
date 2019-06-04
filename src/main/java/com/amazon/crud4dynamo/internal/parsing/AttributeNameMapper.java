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
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;

/**
 * AttributeNameMapper internally holds a map
 *
 * <p>its key is the expression attribute name, e.g, "#HashKey" its value is a list of function,
 * which when applied with attribute name, e.g, "HashKey", return an attribute value converter.
 */
@EqualsAndHashCode
public class AttributeNameMapper {
  private final Map<String, List<Function<String, NameAwareConverter>>> innerMap;

  public AttributeNameMapper() {
    this(new HashMap<>());
  }

  private AttributeNameMapper(final Map<String, List<Function<String, NameAwareConverter>>> map) {
    innerMap = map;
  }

  public static AttributeNameMapper merge(
      final AttributeNameMapper m1, final AttributeNameMapper m2) {
    final Map<String, List<Function<String, NameAwareConverter>>> map =
        MapHelper.merge(m1.innerMap, m2.innerMap).entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, flatList()));
    return new AttributeNameMapper(map);
  }

  private static Function<
          Map.Entry<String, List<List<Function<String, NameAwareConverter>>>>,
          List<Function<String, NameAwareConverter>>>
      flatList() {
    return entry ->
        entry.getValue().stream().flatMap(Collection::stream).collect(Collectors.toList());
  }

  public AttributeNameMapper merge(final AttributeNameMapper other) {
    return merge(this, other);
  }

  public AttributeNameMapper put(
      final String attrName, final Function<String, NameAwareConverter> converterFunction) {
    final List<Function<String, NameAwareConverter>> list =
        innerMap.getOrDefault(attrName, new ArrayList<>());
    list.add(converterFunction);
    innerMap.put(attrName, list);
    return this;
  }

  public boolean has(final String attrName) {
    return innerMap.containsKey(attrName);
  }

  @VisibleForTesting
  Map<String, List<Function<String, NameAwareConverter>>> getInnerMap() {
    return innerMap;
  }

  /**
   * Convert a map whose key is an expression attribute name and value is the corresponding name to
   * AttributeValueMapper.
   */
  public AttributeValueMapper toValueMapper(final Map<String, String> map) {
    final Map<String, AttributeValueConverter> ret =
        map.keySet().stream()
            .filter(innerMap::containsKey)
            .map(toConverters(map))
            .flatMap(Collection::stream)
            .collect(Collectors.toMap(NameAwareConverter::getName, Function.identity()));
    return new AttributeValueMapper(ret);
  }

  private Function<String, List<NameAwareConverter>> toConverters(final Map<String, String> map) {
    return name -> applyName(map, name).apply(innerMap.get(name));
  }

  private Function<List<Function<String, NameAwareConverter>>, List<NameAwareConverter>> applyName(
      final Map<String, String> map, final String name) {
    return funs -> funs.stream().map(fun -> fun.apply(map.get(name))).collect(Collectors.toList());
  }
}
