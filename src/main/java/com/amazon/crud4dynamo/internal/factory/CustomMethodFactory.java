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

import com.amazon.crud4dynamo.annotation.Custom;
import com.amazon.crud4dynamo.exception.CrudForDynamoException;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.factory.ChainedAbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.utility.Reflection;
import java.util.Optional;
import java.util.function.Supplier;

public class CustomMethodFactory extends ChainedAbstractMethodFactory {

    public CustomMethodFactory(final AbstractMethodFactory delegate) {
        super(delegate);
    }

    @Override
    public AbstractMethod create(final Context context) {
        final Optional<Custom> annotation = context.signature().getAnnotation(Custom.class);
        if (!annotation.isPresent()) {
            return super.create(context);
        }
        final AbstractMethodFactory factory =
                annotation.map(Custom::factoryClass).map(Reflection::newInstance).orElseThrow(getException(context));
        return factory.create(context);
    }

    private Supplier<CrudForDynamoException> getException(final Context context) {
        return () -> new CrudForDynamoException("Cannot create factory for method with signature:" + context.signature());
    }
}
