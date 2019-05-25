grammar KeyExpression;

import Common;

start: keyExpression EOF;

keyExpression
 : hashKeyExpression (',' rangeKeyExpression)?
 ;

hashKeyExpression
  : equalityExpression
  ;

rangeKeyExpression
  : equalityExpression
  ;

equalityExpression
 : (EXPRESSION_ATTRIBUTE_NAME| ATTRIBUTE_NAME) '=' EXPRESSION_ATTRIBUTE_VALUE
 ;