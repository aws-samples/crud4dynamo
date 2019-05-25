grammar KeyConditionExpression;

import Common;

start: keyConditionExpression EOF;

/*
 * https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-KeyConditionExpression
 */
keyConditionExpression
  : partitionKeyExpression
  | partitionKeyExpression AND sortKeyExpression
  ;

partitionKeyExpression
  : ATTRIBUTE_NAME '=' EXPRESSION_ATTRIBUTE_VALUE # PartitionKeyExpWithAttrName
  | EXPRESSION_ATTRIBUTE_NAME '=' EXPRESSION_ATTRIBUTE_VALUE # PartitionKeyExpWithExpAttrName
  ;

sortKeyExpression
  : compareExpression
  | betweenExpression
  | beginsWithExpression
  ;

compareExpression
  : ATTRIBUTE_NAME comparator EXPRESSION_ATTRIBUTE_VALUE # CompareExpWithAttrName
  | EXPRESSION_ATTRIBUTE_NAME comparator EXPRESSION_ATTRIBUTE_VALUE # CompareExpWithExpAttrName
  ;

comparator
  : '='
  | '<'
  | '<='
  | '>'
  | '>='
  ;

betweenExpression
  : ATTRIBUTE_NAME BETWEEN EXPRESSION_ATTRIBUTE_VALUE AND EXPRESSION_ATTRIBUTE_VALUE # BetweenExpWithAttrName
  | EXPRESSION_ATTRIBUTE_NAME BETWEEN EXPRESSION_ATTRIBUTE_VALUE AND EXPRESSION_ATTRIBUTE_VALUE # BetweenExpWithExpAttrName
  ;

beginsWithExpression
  : BEGINS_WITH '(' ATTRIBUTE_NAME ',' EXPRESSION_ATTRIBUTE_VALUE ')' # BeginsWithExpWithAttrName
  | BEGINS_WITH '(' EXPRESSION_ATTRIBUTE_NAME ',' EXPRESSION_ATTRIBUTE_VALUE ')' # BeginsWithExpWithExpAttrName
  ;

BEGINS_WITH
  : 'begins_with'
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
