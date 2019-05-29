package com.amazon.crud4dynamo.internal.method;

import com.amazon.crud4dynamo.annotation.Delete;
import com.amazon.crud4dynamo.exception.CrudForDynamoException;
import com.amazon.crud4dynamo.extension.Argument;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.internal.factory.ExpressionFactoryHelper;
import com.amazon.crud4dynamo.internal.parsing.ConditionExpressionParser;
import com.amazon.crud4dynamo.internal.parsing.ExpressionAttributesFactory;
import com.amazon.crud4dynamo.internal.utility.KeyAttributeConstructor;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperTableModel;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.TypeToken;
import java.util.Arrays;
import java.util.List;

class DeleteRequestFactory {
    private final Signature signature;
    private final Class<?> tableClass;
    private final DynamoDBMapper mapper;
    private final Delete deleteAnnotation;
    private final DynamoDBMapperTableModel<?> tableModel;
    private final KeyAttributeConstructor keyAttributeConstructor;
    private final ExpressionAttributesFactory expressionAttributesFactory;

    DeleteRequestFactory(final Signature signature, final Class<?> tableClass, final DynamoDBMapper mapper) {
        this.signature = checkSignatureOrThrow(signature, tableClass);
        this.tableClass = tableClass;
        this.mapper = mapper;
        deleteAnnotation = signature.invokable().getAnnotation(Delete.class);
        tableModel = mapper.getTableModel(tableClass);
        expressionAttributesFactory =
                new ExpressionAttributesFactory(new ConditionExpressionParser(deleteAnnotation.conditionExpression(), tableModel));

        keyAttributeConstructor = new KeyAttributeConstructor(deleteAnnotation.keyExpression(), tableModel);
    }

    public DeleteItemRequest create(final Object... args) {
        final List<Argument> argList = Argument.newList(signature.parameters(), Arrays.asList(args));
        return new DeleteItemRequest()
                .withTableName(ExpressionFactoryHelper.getTableName(tableClass))
                .withKey(keyAttributeConstructor.create(argList))
                .withExpressionAttributeNames(expressionAttributesFactory.newExpressionAttributeNames(argList))
                .withExpressionAttributeValues(expressionAttributesFactory.newExpressionAttributeValues(argList))
                .withConditionExpression(ExpressionFactoryHelper.toNullIfBlank(deleteAnnotation.conditionExpression()))
                .withReturnValues(deleteAnnotation.returnValue());
    }

    private static Signature checkSignatureOrThrow(final Signature signature, final Class<?> modelType) {
        final Delete delete = signature.getAnnotation(Delete.class).orElse(null);
        if (delete == null) {
            throw new NoDeleteAnnotationException(signature);
        }
        final ReturnValue returnValue = delete.returnValue();
        if (!ReturnValue.NONE.equals(returnValue) && !signature.invokable().getReturnType().equals(TypeToken.of(modelType))) {
            throw new ReturnTypeInvalidException(signature, returnValue, modelType);
        }
        return signature;
    }

    @VisibleForTesting
    static class NoDeleteAnnotationException extends CrudForDynamoException {
        private NoDeleteAnnotationException(final Signature signature) {
            super(String.format("Method '%s' is not annotated with @%s", signature, Delete.class.getSimpleName()));
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
