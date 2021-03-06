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

package com.amazon.crud4dynamo.internal.method;

import com.amazon.crud4dynamo.exception.CrudForDynamoException;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.utility.ExceptionHelper;
import com.amazon.crud4dynamo.utility.MethodHandlesHelper;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultMethod implements AbstractMethod {

  private final Method method;
  private final Signature signature;
  private final ConcurrentHashMap<Class<MethodHandle>, MethodHandle> methodHandleMap =
      new ConcurrentHashMap<>();

  public DefaultMethod(final Method method, final Signature signature) {
    this.method = method;
    this.signature = signature;
  }

  private static MethodHandle toMethodHandle(final Method m) {
    final Lookup lookup = MethodHandlesHelper.getLookup(m.getDeclaringClass());
    try {
      return lookup.unreflectSpecial(m, m.getDeclaringClass());
    } catch (final IllegalAccessException e) {
      throw ExceptionHelper.throwAsUnchecked(e);
    }
  }

  @Override
  public Signature getSignature() {
    return signature;
  }

  @Override
  public Object invoke(final Object... args) throws Throwable {
    return Optional.ofNullable(methodHandleMap.get(MethodHandle.class))
        .orElseThrow(
            () ->
                new CrudForDynamoException(
                    "method handle of " + signature + " is not initialized."))
        .invokeWithArguments(args);
  }

  @Override
  public AbstractMethod bind(final Object target) {
    methodHandleMap.computeIfAbsent(
        MethodHandle.class, key -> toMethodHandle(method).bindTo(target));
    return this;
  }
}
