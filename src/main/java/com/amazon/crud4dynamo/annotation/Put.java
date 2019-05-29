package com.amazon.crud4dynamo.annotation;

import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/* https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html#DDB-PutItem-request-Item */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Put {
    /**
     * Specify the expression attribute value for the item to be created. By default it is set to ":item". Besides you have to declare a
     * parameter with Param annotation like <code> @Param(":an_item") final ItemType itemForPut </code>
     */
    String item() default ":item";

    /* https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html */
    String conditionExpression() default "";

    /**
     * Use ReturnValues if you want to get the item attributes as they appear before or after they are updated.
     *
     * <p>https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html#DDB-PutItem-request-ReturnValues
     */
    ReturnValue returnValue() default ReturnValue.NONE;
}
