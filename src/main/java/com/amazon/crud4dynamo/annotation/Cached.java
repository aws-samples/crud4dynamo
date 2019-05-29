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
