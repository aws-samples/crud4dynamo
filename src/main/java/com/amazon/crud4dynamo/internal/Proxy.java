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

package com.amazon.crud4dynamo.internal;

import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.google.common.reflect.AbstractInvocationHandler;
import com.google.common.reflect.Reflection;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Proxy<T> {
  private final Class<T> interfaceType;
  private final Function<Method, AbstractMethod> methodFunction;

  public Proxy(
      final Class<T> interfaceType, final Function<Method, AbstractMethod> methodFunction) {
    this.interfaceType = interfaceType;
    this.methodFunction = methodFunction;
  }

  public T create() {
    final Map<Method, AbstractMethod> dispatchMapping =
        Arrays.stream(interfaceType.getMethods())
            .collect(Collectors.toMap(Function.identity(), methodFunction));
    return Reflection.newProxy(
        interfaceType,
        new AbstractInvocationHandler() {
          @Override
          protected Object handleInvocation(
              final Object o, final Method method, final Object[] objects) throws Throwable {
            try {
              return dispatchMapping.get(method).bind(o).invoke(objects);
            } catch (final InvocationTargetException e) {
              throw e.getTargetException();
            }
          }
        });
  }
}
