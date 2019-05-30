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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

/**
 * Enable caching for method result
 *
 * <p>Details of configuration please check com.google.common.cache.CacheBuilder.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Cached {
    /** Disable by default. */
    int expireAfterAccess() default -1;

    /** Disable by default. */
    int expireAfterWrite() default -1;

    TimeUnit expireAfterAccessTimeUnit() default TimeUnit.MILLISECONDS;

    TimeUnit expireAfterWriteTimeUnit() default TimeUnit.MILLISECONDS;

    int maxSize() default 10;

    int initialCapacity() default 5;

    int concurrencyLevel() default 4;
}
