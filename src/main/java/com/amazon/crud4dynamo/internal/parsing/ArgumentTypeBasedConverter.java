package com.amazon.crud4dynamo.internal.parsing;

import com.amazon.crud4dynamo.exception.CrudForDynamoException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.util.DateUtils;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ArgumentTypeBasedConverter implements AttributeValueConverter {
    private final String path;

    public ArgumentTypeBasedConverter(final String path) {
        this.path = path;
    }

    @Override
    public AttributeValue convert(final Object obj) {
        if (obj instanceof String) {
            return new AttributeValue().withS(String.class.cast(obj));
        } else if (obj instanceof Number) {
            return new AttributeValue().withN(obj.toString());
        } else if (obj instanceof Boolean) {
            return new AttributeValue().withBOOL(Boolean.class.cast(obj));
        } else if (obj instanceof byte[]) {
            final ByteBuffer bf = ByteBuffer.wrap((byte[]) obj);
            return new AttributeValue().withB(bf);
        } else if (obj instanceof ByteBuffer) {
            return new AttributeValue().withB(ByteBuffer.class.cast(obj));
        } else if (obj instanceof Date) {
            final Date date = Date.class.cast(obj);
            return new AttributeValue().withS(DateUtils.formatISO8601Date(date));
        } else if (obj instanceof Calendar) {
            final Calendar calendar = Calendar.class.cast(obj);
            return new AttributeValue().withS(DateUtils.formatISO8601Date(Date.from(calendar.toInstant())));
        } else if (obj instanceof Map) {
            final AttributeValue attributeValue = new AttributeValue();
            final Map<?, ?> amap = (Map<?, ?>) obj;
            for (final Object key : amap.keySet()) {
                String stringKey = null;
                try {
                    stringKey = (String) key;
                } catch (final ClassCastException e) {
                    throw new CrudForDynamoException("Key type of a map should be String.class", e);
                }
                attributeValue.addMEntry(stringKey, convert(amap.get(stringKey)));
            }
            return attributeValue;
        } else if (obj instanceof List) {
            return new AttributeValue().withL(((List<Object>) obj).stream().map(this::convert).collect(Collectors.toList()));
        } else if (obj instanceof Set) {
            final Set<?> aset = (Set<?>) obj;
            if (isStringSet(aset)) {
                return new AttributeValue().withSS(aset.stream().map(this::convert).map(AttributeValue::getS).collect(Collectors.toList()));
            } else if (isNumberSet(aset)) {
                return new AttributeValue().withNS(aset.stream().map(this::convert).map(AttributeValue::getN).collect(Collectors.toList()));
            } else if (isBinarySet(aset)) {
                return new AttributeValue().withBS(aset.stream().map(this::convert).map(AttributeValue::getB).collect(Collectors.toList()));
            } else {
                throw new CrudForDynamoException("DynamoDB only supports String Set, Number Set or Binary Set.");
            }
        } else {
            throw new CrudForDynamoException(String.format("Unsupported type %s for nested attribute %s.", obj.getClass(), path));
        }
    }

    private static boolean isStringSet(final Set<?> aset) {
        return aset.stream().map(Object::getClass).allMatch(String.class::isAssignableFrom);
    }

    private static boolean isNumberSet(final Set<?> aset) {
        return aset.stream().map(Object::getClass).allMatch(Number.class::isAssignableFrom);
    }

    private static boolean isBinarySet(final Set<?> aset) {
        return aset.stream()
                .map(Object::getClass)
                .allMatch(eleClass -> byte[].class.isAssignableFrom(eleClass) || ByteBuffer.class.isAssignableFrom(eleClass));
    }
}
