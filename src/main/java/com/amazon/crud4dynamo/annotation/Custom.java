package com.amazon.crud4dynamo.annotation;

import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Enable custom implementation of user defined method. */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Custom {
    Class<? extends AbstractMethodFactory> factoryClass();
}
