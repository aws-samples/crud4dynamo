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

package com.amazon.crud4dynamo.internal.factory;

import com.amazon.crud4dynamo.annotation.transaction.Get;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.factory.ChainedAbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.internal.method.transaction.TransactionGetMethod;
import com.google.common.collect.ImmutableSet;
import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Set;

public class TransactionGetMethodFactory extends ChainedAbstractMethodFactory {
    private static final Set<Class<? extends Annotation>> TRANSACTION_GET_ANNOTATIONS = ImmutableSet.of(Get.class);

    public TransactionGetMethodFactory(final AbstractMethodFactory delegate) {
        super(delegate);
    }

    @Override
    public AbstractMethod create(final Context context) {
        final Signature signature = context.signature();
        if (TRANSACTION_GET_ANNOTATIONS.stream().map(signature::getAnnotationsByType).allMatch(List::isEmpty)) {
            return super.create(context);
        }
        return new TransactionGetMethod(context.amazonDynamoDb(), context.mapper(), signature);
    }
}
