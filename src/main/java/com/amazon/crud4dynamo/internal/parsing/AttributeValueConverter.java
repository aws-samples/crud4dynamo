package com.amazon.crud4dynamo.internal.parsing;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

public interface AttributeValueConverter {
    AttributeValue convert(final Object object);
}
