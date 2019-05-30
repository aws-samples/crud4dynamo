package com.amazon.crud4dynamo.internal.parsing;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ProjectionExpressionParserTest {
    private static final AttributeNameMapper EMPTY_NAME_MAPPER = new AttributeNameMapper();
    private static final AttributeValueMapper EMPTY_VALUE_MAPPER = new AttributeValueMapper();

    @Test
    void expressionWithoutExpressionAttributeNames() {
        final ProjectionExpressionParser parser = new ProjectionExpressionParser("attributeName1,attributeName2");

        assertThat(parser.getAttributeNameMapper()).isEqualTo(EMPTY_NAME_MAPPER);
        assertThat(parser.getAttributeValueMapper()).isEqualTo(EMPTY_VALUE_MAPPER);
        assertThat(parser.getExpressionAttributeNames()).isEmpty();
    }

    @Test
    void expressionWithExpressionAttributeNames() {
        final ProjectionExpressionParser parser = new ProjectionExpressionParser("attributeName1,#expressionAttributeName1");

        assertThat(parser.getAttributeNameMapper()).isEqualTo(EMPTY_NAME_MAPPER);
        assertThat(parser.getAttributeValueMapper()).isEqualTo(EMPTY_VALUE_MAPPER);
        assertThat(parser.getExpressionAttributeNames()).containsOnly("#expressionAttributeName1");
    }
}
