lexer grammar Common;

/*
 * https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html
 * As attribute name is used to specify a top level field thus "." is not included, see ExpressionAttributeName for more details
 */
ATTRIBUTE_NAME
 : (DIGIT | LETTER | '-' | '_')+
 ;

/**
 * https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html
 */
EXPRESSION_ATTRIBUTE_NAME
 : '#'(LETTER | DIGIT)+
 ;

/**
 * https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeValues.html
 */
EXPRESSION_ATTRIBUTE_VALUE
  : ':'(LETTER | DIGIT)+
  ;

NUMBER : DIGIT+;

WS
  : [ \t\r\n]+ -> skip
  ;

fragment DIGIT : [0-9];
fragment LETTER : [a-zA-Z];
fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];