package com.amazon.crud4dynamo.annotation.transaction;

import com.amazonaws.services.dynamodbv2.model.ReturnValuesOnConditionCheckFailure;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/* https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Update.html */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Repeatable(Updates.class)
public @interface Update {
    Class<?> tableClass();

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
     * Use ReturnValuesOnConditionCheckFailure to get the item attributes if the ConditionCheck condition fails.
     *
     * <p>The valid values are: NONE and ALL_OLD.
     */
    ReturnValuesOnConditionCheckFailure returnValuesOnConditionCheckFailure() default ReturnValuesOnConditionCheckFailure.NONE;
}
