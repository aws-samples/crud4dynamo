package com.amazon.crud4dynamo.internal.factory;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.exception.CrudForDynamoException;
import com.amazon.crud4dynamo.extension.Argument;
import com.amazon.crud4dynamo.extension.PageRequest;
import com.amazon.crud4dynamo.internal.parsing.AttributeValueMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperTableModel;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.base.Strings;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ExpressionFactoryHelper {
    private static final String NAME_PLACEHOLDER_PREFIX = "#";
    private static final String VALUE_PLACEHOLDER_PREFIX = ":";
    private static final Predicate<String> GET_ALL_FILTER = value -> true;

    public static String getTableName(@NonNull final Class<?> tableClass) {
        return Optional.ofNullable(tableClass.getAnnotation(DynamoDBTable.class))
                .map(DynamoDBTable::tableName)
                .orElseThrow(
                        () ->
                                new CrudForDynamoException(
                                        "Class "
                                                + tableClass.getSimpleName()
                                                + " is not annotated with "
                                                + DynamoDBTable.class.getSimpleName()));
    }

    public static String toNullIfBlank(final String str) {
        return Strings.isNullOrEmpty(str) ? null : str;
    }

    /* Traverse argument list and for each argument annotated with @Param("#..."), create a mapping from "#..." to argument string value. */
    public static Map<String, String> getExpressionAttributeNames(@NonNull final List<Argument> args) {
        return getExpressionAttributeNames(args, GET_ALL_FILTER);
    }

    public static Map<String, String> getExpressionAttributeNames(
            @NonNull final List<Argument> args, @NonNull final Predicate<String> paramValueFilter) {
        return getArgsWithAttributeName(args)
                .filter(argument -> paramValueFilter.test(getParamValue().apply(argument)))
                .collect(Collectors.toMap(getParamValue(), getArgumentValue()));
    }

    /**
     * Traverse arguments annotated with @Param(":..."), and for each argument contained in valueMapper creates a mapping from ":..." to
     * AttributeValue.
     */
    public static Map<String, AttributeValue> getExpressionAttributeValues(
            @NonNull final List<Argument> args, @NonNull final AttributeValueMapper valueMapper) {
        return getArgsWithAttributeValue(args)
                .filter(arg -> valueMapper.get(getParamValue(arg).get()) != null)
                .collect(Collectors.toMap(getParamValue(), getAttribute(valueMapper)));
    }

    public static Optional<PageRequest> findPageRequest(final Object... args) {
        return Stream.of(args).filter(PageRequest.class::isInstance).map(PageRequest.class::cast).findFirst();
    }

    public static Map<String, AttributeValue> getLastEvaluatedKey(
            @NonNull final PageRequest<?> pageRequest, @NonNull final DynamoDBMapperTableModel model) {
        return Optional.ofNullable(pageRequest.getExclusiveStartItem())
                .map((Function<Object, Map<String, AttributeValue>>) model::convert)
                .orElse(null);
    }

    private static Function<Argument, AttributeValue> getAttribute(final AttributeValueMapper valueMapper) {
        return arg -> valueMapper.get(getParamValue().apply(arg)).convert(arg.getValue());
    }

    private static Stream<Argument> getArgsWithAttributeName(final List<Argument> args) {
        return args.stream().filter(hasAttributeName());
    }

    private static Stream<Argument> getArgsWithAttributeValue(final List<Argument> args) {
        return args.stream().filter(hasAttributeValue());
    }

    private static Predicate<Argument> hasAttributeName() {
        return hasValueStartsWith(NAME_PLACEHOLDER_PREFIX);
    }

    private static Predicate<Argument> hasAttributeValue() {
        return hasValueStartsWith(VALUE_PLACEHOLDER_PREFIX);
    }

    private static Predicate<Argument> hasValueStartsWith(final String prefix) {
        return arg -> getParamValue(arg).filter(val -> val.startsWith(prefix)).isPresent();
    }

    private static Function<Argument, String> getParamValue() {
        return arg -> arg.getParameter().getAnnotation(Param.class).value();
    }

    private static Optional<String> getParamValue(final Argument arg) {
        return Optional.ofNullable(arg.getParameter().getAnnotation(Param.class)).map(Param::value);
    }

    private static Function<Argument, String> getArgumentValue() {
        return arg -> arg.getValue().toString();
    }
}
