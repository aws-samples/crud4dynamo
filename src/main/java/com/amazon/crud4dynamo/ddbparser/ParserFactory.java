package com.amazon.crud4dynamo.ddbparser;

import java.lang.reflect.Constructor;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.TokenStream;

public class ParserFactory<L extends Lexer, P extends Parser> {
    private final Class<L> lexerType;
    private final Class<P> parserType;

    public ParserFactory(final Class<L> lexerType, final Class<P> parserType) {
        this.lexerType = lexerType;
        this.parserType = parserType;
    }

    public P create(final String input) {
        return newParser(input);
    }

    private P newParser(final String input) {
        final Lexer lexer = newLexer(input);
        final CommonTokenStream commonTokenStream = new CommonTokenStream(lexer);
        try {
            final Constructor<?> constructor = parserType.getConstructor(TokenStream.class);
            final P parser = parserType.cast(constructor.newInstance(commonTokenStream));
            parser.removeErrorListeners();
            parser.addErrorListener(ERROR_LISTENER);
            return parser;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private L newLexer(final String input) {
        try {
            final Constructor<?> constructor = lexerType.getConstructor(CharStream.class);
            final L lexer = lexerType.cast(constructor.newInstance(CharStreams.fromString(input)));
            lexer.removeErrorListeners();
            lexer.addErrorListener(ERROR_LISTENER);
            return lexer;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static final BaseErrorListener ERROR_LISTENER =
            new BaseErrorListener() {
                @Override
                public void syntaxError(
                        final Recognizer<?, ?> recognizer,
                        final Object offendingSymbol,
                        final int line,
                        final int charPositionInLine,
                        final String msg,
                        final RecognitionException e) {
                    throw new RuntimeException("Parsing error: " + msg);
                }
            };
}
