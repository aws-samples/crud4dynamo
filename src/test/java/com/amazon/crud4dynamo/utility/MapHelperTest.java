package com.amazon.crud4dynamo.utility;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MapHelperTest {

    @Test
    void toNull_whenEmpty() {
        final Map<Object, Object> amap = MapHelper.toNullIfEmpty(new HashMap<>());

        assertThat(amap).isNull();
    }

    @Test
    void nonEmpty_returnOriginal() {
        final HashMap<Object, Object> objectObjectHashMap = new HashMap<>();
        objectObjectHashMap.put("dummyKey", "dummyValue");

        final Map<Object, Object> aMap = MapHelper.toNullIfEmpty(objectObjectHashMap);

        assertThat(aMap).isSameAs(objectObjectHashMap);
    }

    @Test
    void merge() {
        final String key = "a";
        final Map<String, List<Integer>> map = MapHelper.merge(ImmutableMap.of(key, 1), ImmutableMap.of(key, 2));

        assertThat(map.get(key)).hasSameElementsAs(Arrays.asList(1, 2));
    }

    @Test
    void mergeWithOverride() {
        final String key = "a";
        final Map<String, Integer> map = MapHelper.overrideMerge(ImmutableMap.of(key, 1), ImmutableMap.of(key, 2));

        assertThat(map.get(key)).isEqualTo(2);
    }
}
