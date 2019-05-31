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
  public static DynamoDBMapperConfig override(
      final DynamoDBMapperConfig base, final DynamoDBMapperConfig overrides) {
    if (base == null || overrides == null) {
      return Optional.ofNullable(base).orElse(overrides);
    }
    try {
      final Method mergeMethod =
          DynamoDBMapperConfig.class.getDeclaredMethod("merge", DynamoDBMapperConfig.class);
      mergeMethod.setAccessible(true);
      return (DynamoDBMapperConfig) mergeMethod.invoke(base, overrides);
    } catch (final NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw ExceptionHelper.throwAsUnchecked(e);
    }
  }
}
