package com.amazon.crud4dynamo.annotation;

import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/* https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Update {
    /**
     * Key Expression has the following syntax.
     *
     * <pre>
     * keyExpression
     *   : equalityExpression (',' equalityExpression)?
     *   ;
     *
     * equalityExpression
     *   : (expressionAttributeName | attributeName) '=' expressionAttributeValue
     *   ;
     * </pre>
     */
    String keyExpression();

    /* https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html#DDB-UpdateItem-request-UpdateExpression */
    String updateExpression();

    /* https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html */
    String conditionExpression() default "";

    /**
     * Use ReturnValues if you want to get the item attributes as they appear before or after they are updated.
     *
     * <p>https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html#API_UpdateItem_RequestSyntax}
     */
    ReturnValue returnValue() default ReturnValue.NONE;
}
