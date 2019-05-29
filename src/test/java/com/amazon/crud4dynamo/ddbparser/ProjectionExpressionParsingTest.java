package com.amazon.crud4dynamo.ddbparser;

import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;
import org.junit.jupiter.api.Test;

public class ProjectionExpressionParsingTest extends ParsingTestBase {
    @Override
    Class<? extends Lexer> getLexerClass() {
        return com.amazon.crud4dynamo.ddbparser.ProjectionExpressionLexer.class;
    }

    @Override
    Class<? extends Parser> getParserClass() {
        return com.amazon.crud4dynamo.ddbparser.ProjectionExpressionParser.class;
    }

    @Override
    String getStartRule() {
        return "start";
    }

    @Test
    void plainAttributeNameOnly() {
        parse("AttributeName", "(start (projectionExpression (attribute AttributeName)) <EOF>)");
    }

    @Test
    void expressionAttributeNameOnly() {
        parse("#expressionAttributeName", "(start (projectionExpression (attribute #expressionAttributeName)) <EOF>)");
    }

    @Test
    void multipleAttributes() {
        parse(
                "attributeName1,#expressionAttributeName1,attributeName2",
                "(start (projectionExpression (attribute attributeName1) , (attribute #expressionAttributeName1) , (attribute attributeName2)) <EOF>)");
    }
}
