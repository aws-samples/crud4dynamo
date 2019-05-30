package com.amazon.crud4dynamo.internal.parsing;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.extension.Argument;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ExpressionAttributesFactoryTest {
    private static final String EXPRESSION_ATTRIBUTE_NAME_1 = "#ExpressionAttributeName1";
    private static final String EXPRESSION_ATTRIBUTE_NAME_2 = "#ExpressionAttributeName2";
    private static final String EXPRESSION_ATTRIBUTE_NAME_3 = "#ExpressionAttributeName3";
    private static final String EXPRESSION_ATTRIBUTE_VALUE_1 = ":ExpressionAttributeValue1";
    private static final String EXPRESSION_ATTRIBUTE_VALUE_2 = ":ExpressionAttributeValue2";
    private static final String EXPRESSION_ATTRIBUTE_VALUE_3 = ":ExpressionAttributeValue3";

    private class TestParser1 implements ExpressionParser {

        @Override
        public AttributeNameMapper getAttributeNameMapper() {
            return new AttributeNameMapper();
        }

        @Override
        public AttributeValueMapper getAttributeValueMapper() {
            return new AttributeValueMapper().put(EXPRESSION_ATTRIBUTE_VALUE_1, object -> new AttributeValue(object.toString()));
        }

        @Override
        public Set<String> getExpressionAttributeNames() {
            return ImmutableSet.of(EXPRESSION_ATTRIBUTE_NAME_1);
        }
    }

    private class TestParser2 implements ExpressionParser {

        @Override
        public AttributeNameMapper getAttributeNameMapper() {
            return new AttributeNameMapper()
                    .put(
                            EXPRESSION_ATTRIBUTE_NAME_3,
                            s ->
                                    new NameAwareConverter() {
                                        @Override
                                        public String getName() {
                                            return EXPRESSION_ATTRIBUTE_VALUE_3;
                                        }

                                        @Override
                                        public AttributeValue convert(final Object object) {
                                            return new AttributeValue(object.toString());
                                        }
                                    });
        }

        @Override
        public AttributeValueMapper getAttributeValueMapper() {
            return new AttributeValueMapper().put(EXPRESSION_ATTRIBUTE_VALUE_2, object -> new AttributeValue(object.toString()));
        }

        @Override
        public Set<String> getExpressionAttributeNames() {
            return ImmutableSet.of(EXPRESSION_ATTRIBUTE_NAME_2, EXPRESSION_ATTRIBUTE_NAME_3);
        }
    }

    private interface TestInterface {
        void methodA(
                @Param(EXPRESSION_ATTRIBUTE_NAME_1) final String expressionAttributeName1,
                @Param(EXPRESSION_ATTRIBUTE_NAME_2) final String expresionAttributeName2,
                @Param(EXPRESSION_ATTRIBUTE_NAME_3) final String expresionAttributeName3,
                @Param(EXPRESSION_ATTRIBUTE_VALUE_1) final String expressionAttributeValue1,
                @Param(EXPRESSION_ATTRIBUTE_VALUE_2) final String expressionAttributeValue2,
                @Param(EXPRESSION_ATTRIBUTE_VALUE_3) final String expressionAttributeValue3);

        static Method getMethodA() throws Exception {
            return TestInterface.class.getMethod(
                    "methodA", String.class, String.class, String.class, String.class, String.class, String.class);
        }

        static List<Argument> newArgumentList(final Method method, final Object... arguments) {
            final Signature signature = Signature.resolve(method, TestInterface.class);
            return Argument.newList(signature.parameters(), Arrays.asList(arguments));
        }
    }

    @Test
    void constructWithSingleParser() throws Exception {
        final List<Argument> arguments =
                TestInterface.newArgumentList(
                        TestInterface.getMethodA(), "name1", "dummyName1", "dummyName2", "value1", "dummyValue2", "dummyValue3");

        final ExpressionAttributesFactory factory = new ExpressionAttributesFactory(new TestParser1());

        assertThat(factory.newExpressionAttributeNames(arguments)).containsEntry(EXPRESSION_ATTRIBUTE_NAME_1, "name1").hasSize(1);
        assertThat(factory.newExpressionAttributeValues(arguments))
                .containsEntry(EXPRESSION_ATTRIBUTE_VALUE_1, new AttributeValue("value1"))
                .hasSize(1);
    }

    @Test
    void constructWithMultipleParsers() throws Exception {
        final List<Argument> arguments =
                TestInterface.newArgumentList(TestInterface.getMethodA(), "name1", "name2", "name3", "value1", "value2", "value3");

        final ExpressionAttributesFactory factory = new ExpressionAttributesFactory(new TestParser1(), new TestParser2());

        assertThat(factory.newExpressionAttributeNames(arguments))
                .containsEntry(EXPRESSION_ATTRIBUTE_NAME_1, "name1")
                .containsEntry(EXPRESSION_ATTRIBUTE_NAME_2, "name2")
                .containsEntry(EXPRESSION_ATTRIBUTE_NAME_3, "name3")
                .hasSize(3);

        assertThat(factory.newExpressionAttributeValues(arguments))
                .containsEntry(EXPRESSION_ATTRIBUTE_VALUE_1, new AttributeValue("value1"))
                .containsEntry(EXPRESSION_ATTRIBUTE_VALUE_2, new AttributeValue("value2"))
                .containsEntry(EXPRESSION_ATTRIBUTE_VALUE_3, new AttributeValue("value3"))
                .hasSize(3);
    }
}
