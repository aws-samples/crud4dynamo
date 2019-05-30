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

import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.utility.ExceptionHelper;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import lombok.Getter;

public class ReflectiveMethod implements AbstractMethod {

    private final Object receiver;
    private final Method method;
    @Getter private final Signature signature;

    public ReflectiveMethod(final Object receiver, final Method method, final Signature signature) {
        this.receiver = receiver;
        this.method = method;
        this.signature = signature;
    }

    @Override
    public Object invoke(final Object... args) {
        try {
            return method.invoke(receiver, args);
        } catch (final IllegalAccessException | InvocationTargetException e) {
            throw ExceptionHelper.throwAsUnchecked(e);
        }
    }

    @Override
    public AbstractMethod bind(final Object target) {
        return this;
    }
}
