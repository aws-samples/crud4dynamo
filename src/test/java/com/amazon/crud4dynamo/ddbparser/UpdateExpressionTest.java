package com.amazon.crud4dynamo.ddbparser;

import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class UpdateExpressionTest {

  abstract class UpdateExpressionTestBase extends ParsingTestBase {

    @Override
    Class<? extends Lexer> getLexerClass() {
      return com.amazon.crud4dynamo.ddbparser.UpdateExpressionLexer.class;
    }

    @Override
    Class<? extends Parser> getParserClass() {
      return com.amazon.crud4dynamo.ddbparser.UpdateExpressionParser.class;
    }

    @Override
    String getStartRule() {
      return "start";
    }
  }

  @Nested
  class SetTest extends UpdateExpressionTestBase {

    @Test
    void case1() {
      parse(
          "SET Integer1 = Integer2",
          "(start (updateExpression (setExpression SET (setAction (path Integer1) = (value (operand (path Integer2)))))) <EOF>)");
    }

    @Test
    void case2() {
      parse(
          "SET #name = :value",
          "(start (updateExpression (setExpression SET (setAction (path #name) = (value (operand :value))))) <EOF>)");
    }

    @Test
    void case3() {
      parse(
          "SET #name1 = #name2",
          "(start (updateExpression (setExpression SET (setAction (path #name1) = (value (operand (path #name2)))))) <EOF>)");
    }

    @Test
    void case4() {
      parseThrowException("SET :value1 = #name");
    }

    @Test
    void case5() {
      parse(
          "SET A.B.C = A + :value",
          "(start (updateExpression (setExpression SET (setAction (path (nestedPath A . (nestedPath B . "
              + "(nestedPath C)))) = (value (operand (path A)) + (operand :value))))) <EOF>)");
    }

    @Test
    void case6() {
      parse(
          "SET A.B.C = A.b - :value",
          "(start (updateExpression (setExpression SET (setAction (path (nestedPath A . (nestedPath B . (nestedPath C)))) = "
              + "(value (operand (path (nestedPath A . (nestedPath b)))) - (operand :value))))) <EOF>)");
    }

    @Test
    void case7() {
      parse(
          "SET #A = #B + #C, #D = #D + :V1, #E = :V2 + :V3, #F = :V4 + E.D",
          "(start (updateExpression (setExpression SET (setAction (path #A) = (value (operand (path #B)) + "
              + "(operand (path #C)))) , (setAction (path #D) = (value (operand (path #D)) + (operand :V1))) , "
              + "(setAction (path #E) = (value (operand :V2) + (operand :V3))) , (setAction (path #F) = "
              + "(value (operand :V4) + (operand (path (nestedPath E . (nestedPath D)))))))) <EOF>)");
    }

    @Test
    void case8() {
      parse(
          "SET A[0][1].B[100].C = #B + #C, #D = #D + :V1, #E = :V2 + :V3, #F = :V4 + E.D",
          "(start (updateExpression (setExpression SET (setAction (path (nestedPath A [0][1] . (nestedPath B [100] . "
              + "(nestedPath C)))) = (value (operand (path #B)) + (operand (path #C)))) , (setAction (path #D) = "
              + "(value (operand (path #D)) + (operand :V1))) , (setAction (path #E) = (value (operand :V2) + "
              + "(operand :V3))) , (setAction (path #F) = (value (operand :V4) + (operand (path (nestedPath E . "
              + "(nestedPath D)))))))) <EOF>)");
    }

    @Test
    void case9() {
      parse(
          "SET #A = list_append(B.A, :ele), #B = if_not_exists(C.D[0], F.G[0])",
          "(start (updateExpression (setExpression SET (setAction (path #A) = (value (operand list_append "
              + "( (operand (path (nestedPath B . (nestedPath A)))) , (operand :ele) )))) , "
              + "(setAction (path #B) = (value (operand if_not_exists ( (path (nestedPath C . "
              + "(nestedPath D [0]))) , (operand (path (nestedPath F . (nestedPath G [0])))) )))))) <EOF>)");
    }
  }

  @Nested
  class RemoveTest extends UpdateExpressionTestBase {

    @Test
    void case1() {
      parse(
          "REMOVE a",
          "(start (updateExpression (removeExpression REMOVE (removeAction (path a)))) <EOF>)");
    }

    @Test
    void case2() {
      parse(
          "REMOVE #a",
          "(start (updateExpression (removeExpression REMOVE (removeAction (path #a)))) <EOF>)");
    }

    @Test
    void case3() {
      parse(
          "REMOVE #a.b[10][0].a[0].#b[1]",
          "(start (updateExpression (removeExpression REMOVE (removeAction (path (nestedPath #a . "
              + "(nestedPath b [10][0] . (nestedPath a [0] . (nestedPath #b [1])))))))) <EOF>)");
    }

    @Test
    void case4() {
      parse(
          "REMOVE a, #a.b.c[0]",
          "(start (updateExpression (removeExpression REMOVE (removeAction (path a)) , (removeAction (path (nestedPath #a . (nestedPath b . (nestedPath c [0]))))))) <EOF>)");
    }
  }

  @Nested
  class AddTest extends UpdateExpressionTestBase {

    @Test
    void case1() {
      parse(
          "ADD a.b :value",
          "(start (updateExpression (addExpression ADD (addAction (path (nestedPath a . (nestedPath b))) :value))) <EOF>)");
    }

    @Test
    void case2() {
      parse(
          "ADD #a :value",
          "(start (updateExpression (addExpression ADD (addAction (path #a) :value))) <EOF>)");
    }

    @Test
    void case3() {
      parse(
          "ADD a.b :value, #b :value2",
          "(start (updateExpression (addExpression ADD (addAction (path "
              + "(nestedPath a . (nestedPath b))) :value) , (addAction (path #b) :value2))) <EOF>)");
    }
  }

  @Nested
  class DeleteTest extends UpdateExpressionTestBase {

    @Test
    void case1() {
      parse(
          "DELETE a :value",
          "(start (updateExpression (deleteExpression DELETE (deleteAction (path a) :value))) <EOF>)");
    }

    @Test
    void case2() {
      parse(
          "DELETE #a :value",
          "(start (updateExpression (deleteExpression DELETE (deleteAction (path #a) :value))) <EOF>)");
    }

    @Test
    void case3() {
      parse(
          "DELETE a :value1, #a :value2",
          "(start (updateExpression (deleteExpression DELETE (deleteAction (path a) :value1) , "
              + "(deleteAction (path #a) :value2))) <EOF>)");
    }
  }
}
