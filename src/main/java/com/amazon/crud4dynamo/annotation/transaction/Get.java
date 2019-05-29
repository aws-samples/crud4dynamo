package com.amazon.crud4dynamo.annotation.transaction;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/* https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Get.html#DDB-Type-Get-ProjectionExpression */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Repeatable(Gets.class)
public @interface Get {
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
     *
     * <p>For example, keyExpression = "HashKey = :hashKeyValue, #rangeKeyName = :rangeKeyValue"
     */
    String keyExpression();

    /**
     * A string that identifies one or more attributes of the specified item to retrieve from the table. The attributes in the expression
     * must be separated by commas. If no attribute names are specified, then all attributes of the specified item are returned. If any of
     * the requested attributes are not found, they do not appear in the result.
     */
    String projectionExpression() default "";
}
