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

import com.amazon.crud4dynamo.exception.CrudForDynamoException;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.factory.ChainedAbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;

public class ThrowingMethodFactory extends ChainedAbstractMethodFactory {
  public ThrowingMethodFactory(final AbstractMethodFactory delegate) {
    super(null);
  }

  @Override
  public AbstractMethod create(final Context context) {
    final String msg =
        String.format(
            "No method factory can respond to method with signature %s%n, interface type %s%n, and raw method %s",
            context.signature(), context.interfaceType(), context.method());
    throw new CrudForDynamoException(msg);
  }
}
