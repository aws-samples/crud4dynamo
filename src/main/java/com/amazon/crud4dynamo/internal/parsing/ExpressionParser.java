package com.amazon.crud4dynamo.internal.parsing;

import com.google.common.collect.ImmutableSet;
import java.util.HashSet;
import java.util.Set;
import lombok.NonNull;

public interface ExpressionParser {
    AttributeNameMapper getAttributeNameMapper();

    AttributeValueMapper getAttributeValueMapper();

    Set<String> getExpressionAttributeNames();

    default ExpressionParser merge(@NonNull final ExpressionParser other) {
        return new ExpressionParser() {
            @Override
            public AttributeNameMapper getAttributeNameMapper() {
                return ExpressionParser.this.getAttributeNameMapper().merge(other.getAttributeNameMapper());
            }

            @Override
            public AttributeValueMapper getAttributeValueMapper() {
                return ExpressionParser.this.getAttributeValueMapper().merge(other.getAttributeValueMapper());
            }

            @Override
            public Set<String> getExpressionAttributeNames() {
                final Set<String> names = new HashSet<>();
                names.addAll(ExpressionParser.this.getExpressionAttributeNames());
                names.addAll(other.getExpressionAttributeNames());
                return names;
            }
        };
    }

    static ExpressionParser newEmptyInstance() {
        return new ExpressionParser() {
            @Override
            public AttributeNameMapper getAttributeNameMapper() {
                return new AttributeNameMapper();
            }

            @Override
            public AttributeValueMapper getAttributeValueMapper() {
                return new AttributeValueMapper();
            }

            @Override
            public Set<String> getExpressionAttributeNames() {
                return ImmutableSet.of();
            }
        };
    }
}
