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

package com.amazon.crud4dynamo.utility;

import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MapHelper {

    public static <K, V> Map<K, V> toNullIfEmpty(@NonNull final Map<K, V> m) {
        return m.isEmpty() ? null : m;
    }

    public static <K, V> Map<K, List<V>> merge(final Map<K, V> m1, final Map<K, V> m2) {
        return Stream.concat(m1.entrySet().stream(), m2.entrySet().stream()).collect(appendEntryToListCollector());
    }

    private static <K, V> Collector<Map.Entry<K, V>, ?, Map<K, List<V>>> appendEntryToListCollector() {
        return Collectors.groupingBy(Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, Collectors.toList()));
    }

    public static <K, V> Map<K, V> overrideMerge(final Map<K, V> m1, final Map<K, V> m2) {
        return Stream.concat(m1.entrySet().stream(), m2.entrySet().stream()).collect(overrideMapEntryCollector());
    }

    private static <K, V> Collector<Map.Entry<K, V>, ?, Map<K, V>> overrideMapEntryCollector() {
        return Collectors.groupingBy(
                Map.Entry::getKey,
                Collectors.mapping(
                        Map.Entry::getValue, Collectors.collectingAndThen(Collectors.toList(), list -> list.get(list.size() - 1))));
    }
}
