package com.amazon.crud4dynamo.internal.method;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.annotation.Put;
import com.amazon.crud4dynamo.exception.CrudForDynamoException;
import com.amazon.crud4dynamo.extension.Argument;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.internal.factory.ExpressionFactoryHelper;
import com.amazon.crud4dynamo.internal.parsing.ConditionExpressionParser;
import com.amazon.crud4dynamo.internal.parsing.ExpressionAttributesFactory;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperTableModel;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.TypeToken;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import lombok.NonNull;

class PutRequestFactory {
    private final Signature signature;
    private final Class<?> modelClass;
    private final DynamoDBMapper dynamoDbMapper;
    private final Put putAnnotation;
    private final DynamoDBMapperTableModel tableModel;
    private final String tableName;
    private final String conditionExpression;
    private final ExpressionAttributesFactory expressionAttributesFactory;

    PutRequestFactory(@NonNull final Signature signature, @NonNull final Class<?> modelClass, @NonNull final DynamoDBMapper dbMapper) {
        this.signature = checkSignatureOrThrow(signature, modelClass);
        this.modelClass = modelClass;
        this.dynamoDbMapper = dbMapper;
        putAnnotation = signature.invokable().getAnnotation(Put.class);
        tableModel = dbMapper.getTableModel(modelClass);
        tableName = ExpressionFactoryHelper.getTableName(modelClass);
        conditionExpression = ExpressionFactoryHelper.toNullIfBlank(putAnnotation.conditionExpression());
        expressionAttributesFactory =
                new ExpressionAttributesFactory(new ConditionExpressionParser(putAnnotation.conditionExpression(), tableModel));
    }

    private static Signature checkSignatureOrThrow(final Signature signature, final Class<?> modelClass) {
        final Optional<Put> putAnnotation = signature.getAnnotation(Put.class);
        if (!putAnnotation.isPresent()) {
            throw new NoPutAnnotationException(signature);
        }
        if (noParameterAnnotatedAsPutItem(signature, putAnnotation.get())) {
            throw new NoPutItemAnnotationException(signature, putAnnotation.get());
        }
        final ReturnValue returnValue = putAnnotation.get().returnValue();
        if (returnValue != ReturnValue.NONE && !signature.invokable().getReturnType().equals(TypeToken.of(modelClass))) {
            throw new ReturnTypeInvalidException(signature, returnValue, modelClass);
        }
        return signature;
    }

    private static boolean noParameterAnnotatedAsPutItem(final Signature signature, final Put putAnnotation) {
        return signature
                .invokable()
                .getParameters()
                .stream()
                .map(p -> p.getAnnotation(Param.class))
                .filter(Objects::nonNull)
                .noneMatch(p -> p.value().equals(putAnnotation.item()));
    }

    public PutItemRequest create(final Object... args) {
        final List<Argument> argList = Argument.newList(signature.parameters(), Arrays.asList(args));
        return new PutItemRequest()
                .withTableName(tableName)
                .withItem(getItem(argList))
                .withConditionExpression(conditionExpression)
                .withExpressionAttributeNames(expressionAttributesFactory.newExpressionAttributeNames(argList))
                .withExpressionAttributeValues(expressionAttributesFactory.newExpressionAttributeValues(argList))
                .withReturnValues(putAnnotation.returnValue());
    }

    @SuppressWarnings("unchecked")
    private Map<String, AttributeValue> getItem(final List<Argument> argList) {
        return tableModel.convert(getPutItem(argList));
    }

    private Object getPutItem(final List<Argument> args) {
        return args.stream().filter(filterArgumentByPutItem()).findFirst().map(Argument::getValue).get();
    }

    private Predicate<Argument> filterArgumentByPutItem() {
        return arg ->
                Optional.ofNullable(arg.getParameter().getAnnotation(Param.class))
                        .map(Param::value)
                        .filter(putAnnotation.item()::equals)
                        .isPresent();
    }

    @VisibleForTesting
    static class NoPutAnnotationException extends CrudForDynamoException {
        private NoPutAnnotationException(final Signature signature) {
            super(String.format("Method '%s' is not annotated with @%s", signature, Put.class.getSimpleName()));
        }
    }

    @VisibleForTesting
    static class NoPutItemAnnotationException extends CrudForDynamoException {
        private NoPutItemAnnotationException(final Signature signature, final Put put) {
            super(String.format("Method '%s' does not have a parameter annotated with @Param(%s)", signature, put.item()));
        }
    }

    @VisibleForTesting
    static class ReturnTypeInvalidException extends CrudForDynamoException {
        private ReturnTypeInvalidException(final Signature signature, final ReturnValue returnValue, final Class<?> modelType) {
            super(
                    String.format(
                            "Method '%s' with ReturnValue set to '%s' should use %s as return type.",
                            signature, returnValue, modelType.getSimpleName()));
        }
    }
}
