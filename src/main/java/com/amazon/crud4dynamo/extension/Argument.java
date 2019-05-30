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

import com.google.common.base.Preconditions;
import com.google.common.reflect.Parameter;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Value;

@Value
public class Argument {
    private Parameter parameter;
    private Object value;

    public static List<Argument> newList(final List<Parameter> parameters, final List<Object> arguments) {
        Preconditions.checkArgument(
                parameters.size() == arguments.size(), "Number of parameters should equal to actual number method arguments.");
        return IntStream.range(0, parameters.size())
                .mapToObj(i -> new Argument(parameters.get(i), arguments.get(i)))
                .collect(Collectors.toList());
    }
}
