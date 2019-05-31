package com.amazon.crud4dynamo.ddbparser;

import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ConditionExpressionParsingTest extends ParsingTestBase {
  @Override
  Class<? extends Lexer> getLexerClass() {
    return com.amazon.crud4dynamo.ddbparser.ConditionExpressionLexer.class;
  }

  @Override
  Class<? extends Parser> getParserClass() {
    return com.amazon.crud4dynamo.ddbparser.ConditionExpressionParser.class;
  }

  @Override
  String getStartRule() {
    return "start";
  }

  @ParameterizedTest
  @ValueSource(strings = {"=", "<>", "<", "<=", ">", ">="})
  void leftCompareExpressionHappyCase(final String comparator) {
    parse(
        String.format("#IntAttribute %s :value", comparator),
        "(start (conditionExpression (leftComparisonExpression (path #IntAttribute) "
            + String.format("(comparator %s)", comparator)
            + " :value)) <EOF>)");
  }

  @Test
  void leftCompareExpressionErrorCase() {
    parseThrowException("#IntAttribute != :value");
  }

  @ParameterizedTest
  @ValueSource(strings = {"=", "<>", "<", "<=", ">", ">="})
  void rightCompareExpressionHappyCase(final String comparator) {
    parse(
        String.format(":value %s #IntAttribute", comparator),
        "(start (conditionExpression (rightComparisonExpression :value "
            + String.format("(comparator %s)", comparator)
            + " (path #IntAttribute))) <EOF>)");
  }

  @Test
  void betweenExpressionHappyCase() {
    parse(
        "#IntAttribute between :value1 and :value2",
        "(start (conditionExpression (path #IntAttribute) between :value1 and :value2) <EOF>)");
  }

  @Test
  void betweenExpressionErrorCase() {
    parseThrowException("#IntAttribute between :value1 and :value2 and :value3");
  }

  @Test
  void InExpressionHappyCase() {
    parse(
        "#attr IN (:val1, :val2, :val3)",
        "(start (conditionExpression (path #attr) IN ( :val1 , :val2 , :val3 )) <EOF>)");
  }

  @Test
  void InExpressionHappyErrorCase() {
    parseThrowException("#attr IN (:val1, :val2, :val3 :val4)");
  }

  @Test
  void attributeExistsFunctionHappyCase() {
    parse(
        "attribute_exists(A.b)",
        "(start (conditionExpression (functionExpression attribute_exists ( (path (nestedPath A . (nestedPath b))) ))) <EOF>)");
  }

  @Test
  void attributeExistsFunctionErrorCase() {
    parseThrowException("Attribute_exists(A.b)");
  }

  @Test
  void attributeNotExistsFunctionHappyCase() {
    parse(
        "attribute_not_exists(A.b)",
        "(start (conditionExpression (functionExpression attribute_not_exists ( (path (nestedPath A . (nestedPath b))) ))) <EOF>)");
  }

  @Test
  void attributeNotExistsFunctionErrorCase() {
    parseThrowException("Attribute_not_exists(A.b)");
  }

  @Test
  void attributeTypeFunctionHappyCase() {
    parse(
        "attribute_type(A, :type)",
        "(start (conditionExpression (functionExpression attribute_type ( (path A) , :type ))) <EOF>)");
  }

  @Test
  void attributeTypeFunctionErrorCase() {
    parseThrowException("Attribute_type(A, :type)");
  }

  @Test
  void beginsWithFunctionHappyCase() {
    parse(
        "begins_with(#abc, :value)",
        "(start (conditionExpression (functionExpression begins_with ( (path #abc) , :value ))) <EOF>)");
  }

  @Test
  void beginsWithFunctionErrorCase() {
    parseThrowException("Begins_with(#abc, :value)");
  }

  @Test
  void containsExpressionHappyCase() {
    parse(
        "contains(A, :val)",
        "(start (conditionExpression (functionExpression contains ( (path A) , :val ))) <EOF>)");
  }

  @Test
  void containsExpressionErrorCase() {
    parseThrowException("Contains(A, :val)");
  }

  @Test
  void sizeFunctionHappyCase() {
    parse("size(A)", "(start (conditionExpression (functionExpression size ( (path A) ))) <EOF>)");
  }

  @Test
  void sizeFunctionErrorCase() {
    parseThrowException("Size(A)");
  }

  @Test
  void groupExpressionHappyCase() {
    parse(
        "(#a = :val)",
        "(start (conditionExpression ( (conditionExpression (leftComparisonExpression (path #a) (comparator =) :val)) )) <EOF>)");
  }

  @Test
  void groupExpressionErrorCase() {
    parseThrowException("((#a = :val)");
  }

  @Test
  void notExpressionHappyCase() {
    parse(
        "NOT #a = :val",
        "(start (conditionExpression NOT (conditionExpression (leftComparisonExpression (path #a) (comparator =) :val))) <EOF>)");
  }

  @Test
  void notExpressionErrorCase() {
    parseThrowException("NOTNOT #a = :val");
  }

  @Test
  void andExpressionHappyCase() {
    parse(
        "#a = :val and NOT #a = :val",
        "(start (conditionExpression (conditionExpression (leftComparisonExpression (path #a) (comparator =) :val)) and "
            + "(conditionExpression NOT (conditionExpression (leftComparisonExpression (path #a) (comparator =) :val)))) <EOF>)");
  }

  @Test
  void andExpressionErrorCase() {
    parseThrowException("#a = :val andNOT #a = :val");
  }

  @Test
  void orExpressionHappyCase() {
    parse(
        "#a = :val or NOT #a = :val",
        "(start (conditionExpression (conditionExpression (leftComparisonExpression (path #a) (comparator =) :val)) or "
            + "(conditionExpression NOT (conditionExpression (leftComparisonExpression (path #a) (comparator =) :val)))) <EOF>)");
  }

  @Test
  void orExpressionErrorCase() {
    parseThrowException("#a = :val orNOT #a = :val");
  }

  @Test
  void complexPath1() {
    parse(
        "A[1][222][3].B[3].c = :value",
        "(start (conditionExpression (leftComparisonExpression "
            + "(path (nestedPath A [1][222][3] . (nestedPath B [3] . (nestedPath c)))) (comparator =) :value)) <EOF>)");
  }

  @Test
  void complexPath2() {
    parse(
        "A[1].#attrName1.#attrName2.#a[0].Integer = :value",
        "(start (conditionExpression (leftComparisonExpression (path (nestedPath A [1] . (nestedPath #attrName1 . "
            + "(nestedPath #attrName2 . (nestedPath #a [0] . (nestedPath Integer)))))) (comparator =) :value)) <EOF>)");
  }

  /**
   * Parse tree should have the following structure OrExp( AndExp(NotExp,LeftCompExp), LeftCompExp )
   */
  @Test
  void complexLogicExpression1() {
    parse(
        "NOT a = :a AND b = :b OR c = :c",
        "(start (conditionExpression (conditionExpression "
            + "(conditionExpression NOT (conditionExpression (leftComparisonExpression (path a) (comparator =) :a))) "
            + "AND (conditionExpression (leftComparisonExpression (path b) (comparator =) :b))) "
            + "OR (conditionExpression (leftComparisonExpression (path c) (comparator =) :c))) <EOF>)");
  }

  /**
   * Parse tree has the following structure: OrExp( LeftCompExp, AndExp(LeftCompExp, LeftCompExp) )
   */
  @Test
  void complexLogicExpression2() {
    parse(
        "a = :a OR b = :b AND c = :c",
        "(start (conditionExpression (conditionExpression (leftComparisonExpression (path a) (comparator =) :a)) OR "
            + "(conditionExpression (conditionExpression (leftComparisonExpression (path b) (comparator =) :b)) AND "
            + "(conditionExpression (leftComparisonExpression (path c) (comparator =) :c)))) <EOF>)");
  }
}
