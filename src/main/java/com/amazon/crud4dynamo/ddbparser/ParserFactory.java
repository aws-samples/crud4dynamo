/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

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
}
