package com.amazon.crud4dynamo.ddbparser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;

public abstract class ParsingTestBase {
    abstract Class<? extends Lexer> getLexerClass();

    abstract Class<? extends Parser> getParserClass();

    abstract String getStartRule();

    protected void parse(final String input, final String expectedParsedTree) {
        assertThat(doParsing(input)).isEqualTo(expectedParsedTree);
    }

    protected void parseThrowException(final String input) {
        assertThatThrownBy(() -> doParsing(input)).hasRootCauseInstanceOf(RuntimeException.class).hasMessageContaining("Parsing error:");
    }

    private String doParsing(final String input) {
        final Parser parser = new ParserFactory<>(getLexerClass(), getParserClass()).create(input);
        try {
            final Method expression = parser.getClass().getMethod(getStartRule());
            final ParserRuleContext context = ParserRuleContext.class.cast(expression.invoke(parser));
            return context.toStringTree(parser);
        } catch (final InvocationTargetException e) {
            throw new RuntimeException(e.getCause());
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }
}
