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

import com.amazon.crud4dynamo.annotation.Scan;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.PageResult;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.factory.ChainedAbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.internal.method.scan.NonPagingMethod;
import com.amazon.crud4dynamo.internal.method.scan.PagingMethod;

public class ScanMethodFactory extends ChainedAbstractMethodFactory {
    public ScanMethodFactory(final AbstractMethodFactory delegate) {
        super(delegate);
    }

    @Override
    public AbstractMethod create(final Context context) {
        if (!isAnnotatedWithScan(context.signature())) {
            return super.create(context);
        }
        return requirePaging(context)
                ? new PagingMethod(context.signature(), context.modelType(), context.mapper(), context.mapperConfig())
                : new NonPagingMethod(context.signature(), context.modelType(), context.mapper(), context.mapperConfig());
    }

    private boolean requirePaging(final Context context) {
        return context.signature().invokable().getReturnType().isSubtypeOf(PageResult.class);
    }

    private boolean isAnnotatedWithScan(final Signature signature) {
        return signature.invokable().isAnnotationPresent(Scan.class);
    }
}
