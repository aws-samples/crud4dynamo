// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html
grammar UpdateExpression;

import Common;

start: updateExpression EOF;

updateExpression
  : setExpression
  | removeExpression
  | addExpression
  | deleteExpression
  ;

setExpression
  : SET setAction (',' setAction)*
  ;

removeExpression
  : REMOVE removeAction (',' removeAction)*
  ;

addExpression
  : ADD addAction (',' addAction)*
  ;

deleteExpression
  : DELETE deleteAction (',' deleteAction)*
  ;

setAction
  : path '=' value
  ;

removeAction
  : path
  ;

addAction
  : path EXPRESSION_ATTRIBUTE_VALUE
  ;

deleteAction
  : path EXPRESSION_ATTRIBUTE_VALUE
  ;

value
  : operand
  | operand '+' operand
  | operand '-' operand
  ;

operand
  : path
  | EXPRESSION_ATTRIBUTE_VALUE
  | IF_NOT_EXISTS '(' path ',' operand ')'
  | LIST_APPEND '(' operand ',' operand ')'
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

IF_NOT_EXISTS
  : 'if_not_exists'
  ;

LIST_APPEND
  : 'list_append'
  ;

SET
  : S E T
  ;

REMOVE
  : R E M O V E
  ;

ADD
  : A D D
  ;

DELETE
  : D E L E T E
  ;

INDEXING_OPERATOR
  : ('[' NUMBER ']')+
  ;

