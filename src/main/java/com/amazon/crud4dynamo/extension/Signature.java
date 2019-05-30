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

package com.amazon.crud4dynamo.extension;

import com.google.common.reflect.Invokable;
import com.google.common.reflect.Parameter;
import com.google.common.reflect.TypeToken;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Builder
@Accessors(fluent = true, chain = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Signature {
    private Invokable<?, Object> invokable;
    private TypeToken<?> returnType;
    private List<Parameter> parameters;
    private String methodName;
    private Method method;
    private String string;

    /** Given a method retrieved from generic superclass, resolve its generic type parameter based on a parameterized subclass. */
    public static Signature resolve(final Method method, final Class<?> parameterizedType) {
        final Invokable<?, Object> invokable = TypeToken.of(parameterizedType).method(method);
        return Signature.builder()
                .string(formatSignatureString(method, invokable))
                .methodName(method.getName())
                .parameters(invokable.getParameters())
                .returnType(invokable.getReturnType())
                .invokable(invokable)
                .method(method)
                .build();
    }

    private static String formatSignatureString(final Method method, final Invokable<?, Object> invokable) {
        return String.format(
                "%s %s(%s)",
                invokable.getReturnType(),
                method.getName(),
                getParameterTypes(invokable).map(TypeToken::toString).collect(Collectors.joining(",")));
    }

    private static Stream<? extends TypeToken<?>> getParameterTypes(final Invokable<?, Object> invokable) {
        return invokable.getParameters().stream().map(Parameter::getType);
    }

    public <T extends Annotation> Optional<T> getAnnotation(final Class<T> type) {
        return Optional.ofNullable(invokable.getAnnotation(type));
    }

    /* Call to invokable.getAnnotationsByType throws exception so calling method.getAnnotationsByType as a workaround */
    public <T extends Annotation> List<T> getAnnotationsByType(final Class<T> type) {
        return Arrays.asList(method.getAnnotationsByType(type));
    }

    @Override
    public String toString() {
        return string;
    }
}
