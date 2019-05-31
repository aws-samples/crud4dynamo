package com.amazon.crud4dynamo.ddbparser;

import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;
import org.junit.jupiter.api.Test;

class KeyExpression extends ParsingTestBase {

  @Override
  Class<? extends Lexer> getLexerClass() {
    return com.amazon.crud4dynamo.ddbparser.KeyExpressionLexer.class;
  }

  @Override
  Class<? extends Parser> getParserClass() {
    return com.amazon.crud4dynamo.ddbparser.KeyExpressionParser.class;
  }

  @Override
  String getStartRule() {
    return "start";
  }

  @Test
  void simpleKey_case1() {
    parse(
        "hashKey = :value",
        "(start (keyExpression (hashKeyExpression (equalityExpression hashKey = :value))) <EOF>)");
  }

  @Test
  void simpleKey_case2() {
    parse(
        "#hashKey = :value",
        "(start (keyExpression (hashKeyExpression (equalityExpression #hashKey = :value))) <EOF>)");
  }

  @Test
  void compositeKey_case1() {
    parse(
        "hashKey = :value, rangeKey = :value",
        "(start (keyExpression (hashKeyExpression (equalityExpression hashKey = :value)) , "
            + "(rangeKeyExpression (equalityExpression rangeKey = :value))) <EOF>)");
  }

  @Test
  void compositeKey_case2() {
    parse(
        "#hashKey = :value, rangeKey = :value",
        "(start (keyExpression (hashKeyExpression (equalityExpression #hashKey = :value)) , "
            + "(rangeKeyExpression (equalityExpression rangeKey = :value))) <EOF>)");
  }
}
