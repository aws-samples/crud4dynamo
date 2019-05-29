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
