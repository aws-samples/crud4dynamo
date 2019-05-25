grammar ConditionExpression;

import Common;

start: conditionExpression EOF;

/*
 * https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
 */
conditionExpression
  : leftComparisonExpression # LeftCompExp
  | rightComparisonExpression # RightCompExp
  | path BETWEEN EXPRESSION_ATTRIBUTE_VALUE AND EXPRESSION_ATTRIBUTE_VALUE # BetweenExp
  | path IN '(' EXPRESSION_ATTRIBUTE_VALUE (',' EXPRESSION_ATTRIBUTE_VALUE)* ')' # InExp
  | functionExpression # FunExp
  | '(' conditionExpression ')' # GroupExp
  | NOT conditionExpression # NotExp
  | conditionExpression AND conditionExpression # AndExp
  | conditionExpression OR conditionExpression # OrExp
  ;

leftComparisonExpression
  : path comparator EXPRESSION_ATTRIBUTE_VALUE
  ;

rightComparisonExpression
  : EXPRESSION_ATTRIBUTE_VALUE comparator path
  ;

functionExpression
  : ATTRIBUTE_EXISTS '(' path ')' # AttrExistsFunExp
  | ATTRIBUTE_NOT_EXISTS '(' path ')' # AttrNotExistsFunExp
  | ATTRIBUTE_TYPE '(' path ',' EXPRESSION_ATTRIBUTE_VALUE ')' # AttrTypeFunExp
  | BEGINS_WITH '(' path ',' EXPRESSION_ATTRIBUTE_VALUE ')' # BeginsWithFunExp
  | CONTAINS '(' path ',' EXPRESSION_ATTRIBUTE_VALUE ')' # ContainsFunExp
  | SIZE '(' path ')' # SizeFunExp
  ;

comparator
  : '='
  | '<>'
  | '<'
  | '<='
  | '>'
  | '>='
  ;

path
  : ATTRIBUTE_NAME
  | EXPRESSION_ATTRIBUTE_NAME
  | nestedPath
  ;

nestedPath
  : (ATTRIBUTE_NAME | EXPRESSION_ATTRIBUTE_NAME) '.' nestedPath
  | (ATTRIBUTE_NAME | EXPRESSION_ATTRIBUTE_NAME) INDEXING_OPERATOR
  | (ATTRIBUTE_NAME | EXPRESSION_ATTRIBUTE_NAME) INDEXING_OPERATOR '.' nestedPath
  | (ATTRIBUTE_NAME | EXPRESSION_ATTRIBUTE_NAME)
  ;

BEGINS_WITH
  : 'begins_with'
  ;

ATTRIBUTE_EXISTS
  : 'attribute_exists'
  ;

ATTRIBUTE_NOT_EXISTS
  : 'attribute_not_exists'
  ;

ATTRIBUTE_TYPE
  : 'attribute_type'
  ;

CONTAINS
  : 'contains'
  ;

SIZE
  : 'size'
  ;

BETWEEN
  : B E T W E E N
  ;

AND
  : A N D
  ;

OR
  : O R
  ;

NOT
  : N O T
  ;

IN
  : I N
  ;

INDEXING_OPERATOR
  : ('[' NUMBER ']')+
  ;
