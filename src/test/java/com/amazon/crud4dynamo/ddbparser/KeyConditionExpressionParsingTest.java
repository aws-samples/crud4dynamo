package com.amazon.crud4dynamo.ddbparser;

import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class KeyConditionExpressionParsingTest extends ParsingTestBase {
  @Override
  Class<? extends Lexer> getLexerClass() {
    return com.amazon.crud4dynamo.ddbparser.KeyConditionExpressionLexer.class;
  }

  @Override
  Class<? extends Parser> getParserClass() {
    return com.amazon.crud4dynamo.ddbparser.KeyConditionExpressionParser.class;
  }

  @Override
  String getStartRule() {
    return "start";
  }

  @Test
  void partitionKeyHappyCase() {
    parse(
        "#HashKey = :hashKey",
        "(start (keyConditionExpression (partitionKeyExpression #HashKey = :hashKey)) <EOF>)");
  }

  @Test
  void partitionKeyErrorCase() {
    parseThrowException("#HashKey > :hashKey");
  }

  @ParameterizedTest
  @ValueSource(strings = {"=", "<", "<=", ">", ">="})
  void compareExpressionHappy(final String comparator) {
    parse(
        String.format("#HashKey = :hashKey AND RangeKey %s :rangeKey", comparator),
        "(start (keyConditionExpression (partitionKeyExpression #HashKey = :hashKey) "
            + "AND (sortKeyExpression (compareExpression RangeKey "
            + String.format("(comparator %s)", comparator)
            + " :rangeKey))) <EOF>)");
  }

  @Test
  void compareExpressionErrorCase() {
    parseThrowException("#HashKey = :hashKey AND RangeKey <> :rangeKey");
  }

  @Test
  void betweenExpressionHappyCase() {
    parse(
        "#HashKey = :hashKey AND RangeKey BETWEEN :rangeKey1 AND :rangeKey2",
        "(start (keyConditionExpression (partitionKeyExpression #HashKey = :hashKey) "
            + "AND (sortKeyExpression (betweenExpression RangeKey BETWEEN :rangeKey1 AND :rangeKey2))) <EOF>)");
  }

  @Test
  void betweenExpressionErrorCase() {
    parseThrowException("#HashKey = :hashKey AND RangeKey BETWEENs :rangeKey1 AND :rangeKey2");
  }

  @Test
  void withBeginsWithExpressionHappyCase() {
    parse(
        "#HashKey = :hashKey AND begins_with(#RangeKey, :value)",
        "(start (keyConditionExpression (partitionKeyExpression #HashKey = :hashKey) "
            + "AND (sortKeyExpression (beginsWithExpression begins_with ( #RangeKey , :value )))) <EOF>)");
  }

  @Test
  void withBeginsWithExpressionErrorCase() {
    parseThrowException("#HashKey = :hashKey AND begins_with(#RangeKey, :value1, :value2)");
  }
}
