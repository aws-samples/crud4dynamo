package com.amazon.crud4dynamo.internal.parsing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class AttributeNameMapperTest {
  private final AttributeNameMapper mapper = new AttributeNameMapper();

  @Test
  void put() {
    final Function function = mock(Function.class);
    final String key = "KEY";
    mapper.put(key, function);

    assertThat(mapper.getInnerMap()).containsOnlyKeys(key);
    assertThat(mapper.getInnerMap()).containsValue(Arrays.asList(function));
  }

  @Test
  void merge() {
    final Function function = mock(Function.class);
    final String key = "KEY";
    final AttributeNameMapper mergedMapper =
        mapper.put(key, function).merge(new AttributeNameMapper().put(key, function));

    assertThat(mergedMapper.getInnerMap()).containsOnlyKeys(key);
    assertThat(mergedMapper.getInnerMap()).containsValue(Arrays.asList(function, function));
  }

  @Test
  void toValueMapper() {
    final String expressionAttributeName = "#HashKey";
    final String expressionAttributeValue = ":value";
    final String attributeName = "HashKey";
    final NameAwareConverter nameAwareConverter = mock(NameAwareConverter.class);
    when(nameAwareConverter.getName()).thenReturn(expressionAttributeValue);
    final Function function = mock(Function.class);
    when(function.apply(attributeName)).thenReturn(nameAwareConverter);

    final AttributeValueMapper valueMapper =
        mapper
            .put(expressionAttributeName, function)
            .toValueMapper(ImmutableMap.of(expressionAttributeName, attributeName));

    assertThat(valueMapper.get(expressionAttributeValue)).isEqualTo(nameAwareConverter);
  }
}
