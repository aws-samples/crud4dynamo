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

import com.amazon.crud4dynamo.annotation.Put;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.factory.ChainedAbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.internal.method.PutMethod;

public class PutMethodFactory extends ChainedAbstractMethodFactory {
    public PutMethodFactory(final AbstractMethodFactory delegate) {
        super(delegate);
    }

    @Override
    public AbstractMethod create(final Context context) {
        if (isAnnotatedWithPut(context)) {
            return new PutMethod(
                    context.signature(), context.modelType(), context.mapper(), context.amazonDynamoDb(), context.mapperConfig());
        }
        return super.create(context);
    }

    private static boolean isAnnotatedWithPut(final Context context) {
        return context.signature().invokable().isAnnotationPresent(Put.class);
    }
}
