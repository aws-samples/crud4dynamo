package com.amazon.crud4dynamo.internal.parsing;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class AttributeValueMapperTest {
    private final AttributeValueMapper valueMapper = new AttributeValueMapper();

    @Test
    void putAndGet() {
        final AttributeValueConverter converter = mock(AttributeValueConverter.class);
        final String key = "KEY";
        valueMapper.put(key, converter);

        assertThat(valueMapper.get(key)).isEqualTo(converter);
    }

    @Test
    void merge() {
        final AttributeValueConverter converter1 = mock(AttributeValueConverter.class);
        final AttributeValueConverter converter2 = mock(AttributeValueConverter.class);
        final String key1 = "KEY1";
        final String key2 = "KEY2";
        final AttributeValueMapper mergedValueMapper =
                valueMapper.put(key1, converter1).merge(new AttributeValueMapper().put(key2, converter2));

        assertThat(mergedValueMapper.get(key1)).isEqualTo(converter1);
        assertThat(mergedValueMapper.get(key2)).isEqualTo(converter2);
    }
}
