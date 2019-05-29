package com.amazon.crud4dynamo.internal.utility;

import com.amazon.crud4dynamo.utility.ExceptionHelper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DynamoDbMapperConfigHelper {
    public static DynamoDBMapperConfig override(final DynamoDBMapperConfig base, final DynamoDBMapperConfig overrides) {
        if (base == null || overrides == null) {
            return Optional.ofNullable(base).orElse(overrides);
        }
        try {
            final Method mergeMethod = DynamoDBMapperConfig.class.getDeclaredMethod("merge", DynamoDBMapperConfig.class);
            mergeMethod.setAccessible(true);
            return (DynamoDBMapperConfig) mergeMethod.invoke(base, overrides);
        } catch (final NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw ExceptionHelper.throwAsUnchecked(e);
        }
    }
}
