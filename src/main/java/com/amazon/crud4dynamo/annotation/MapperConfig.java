package com.amazon.crud4dynamo.annotation;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.ConsistentReads;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.SaveBehavior;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Override DynamoDB Mapper Config
 *
 * <p>This is used with basic CRUD operations.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface MapperConfig {

    /**
     * https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/datamodeling/DynamoDBMapperConfig.SaveBehavior.html
     */
    SaveBehavior saveBehavior() default SaveBehavior.UPDATE;

    /** https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html */
    ConsistentReads consistentReads() default ConsistentReads.EVENTUAL;
}
