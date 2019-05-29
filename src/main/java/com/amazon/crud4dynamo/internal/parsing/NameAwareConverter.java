package com.amazon.crud4dynamo.internal.parsing;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperTableModel;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import java.util.function.Function;

/** An attribute value converter which is aware of the expression attribute name, e.g, ":value" */
public interface NameAwareConverter extends AttributeValueConverter {
    String getName();

    static Function<String, NameAwareConverter> newLazyConverter(final String attributeValue, final DynamoDBMapperTableModel tableModel) {
        return name ->
                new NameAwareConverter() {
                    @Override
                    public String getName() {
                        return attributeValue;
                    }

                    @Override
                    public AttributeValue convert(final Object object) {
                        return tableModel.field(name).convert(object);
                    }
                };
    }
}
