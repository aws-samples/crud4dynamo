package com.amazon.crud4dynamo.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Use <code>@Param</code> to name expression attribute names and expression attribute values.
 *
 * <p>@Param("#expressionAttributeName")
 *
 * <p>@Param(":expressionAttributeValue")
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface Param {
    String value() default "";
}
