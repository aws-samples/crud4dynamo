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
 * <p>its value is a attribute value converter, when applied it converts the argument to its corresponding DynamoDB attribute type.
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

    public static AttributeValueMapper merge(final AttributeValueMapper m1, final AttributeValueMapper m2) {
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
