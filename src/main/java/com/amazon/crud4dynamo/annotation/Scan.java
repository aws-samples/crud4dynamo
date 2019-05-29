package com.amazon.crud4dynamo.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Scan {
    /**
     * A filter expression determines which items within the results should be returned.
     *
     * <p>The syntax for a filter expression is identical to that of a condition expression.
     *
     * <p>https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
     */
    String filter();

    /** https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SecondaryIndexes.html */
    String index() default "";

    /** https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-ConsistentRead */
    boolean consistentRead() default false;
}