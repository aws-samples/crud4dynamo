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

package com.amazon.crud4dynamo.extension.factory;

import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;

public abstract class ChainedAbstractMethodFactory implements AbstractMethodFactory {

  private final AbstractMethodFactory delegate;

  public ChainedAbstractMethodFactory(final AbstractMethodFactory delegate) {
    this.delegate = delegate;
  }

  @Override
  public AbstractMethod create(final Context context) {
    if (null != delegate) {
      return delegate.create(context);
    }
    return null;
  }
}
