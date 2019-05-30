/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

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
