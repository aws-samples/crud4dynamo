grammar ProjectionExpression;

import Common;

start: projectionExpression EOF;

projectionExpression
    : attribute (',' attribute)*
    ;

attribute
    : ATTRIBUTE_NAME
    | EXPRESSION_ATTRIBUTE_NAME
    ;