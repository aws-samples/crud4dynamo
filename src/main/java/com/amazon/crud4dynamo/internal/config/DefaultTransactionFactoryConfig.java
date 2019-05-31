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

package com.amazon.crud4dynamo.internal.config;

import com.amazon.crud4dynamo.extension.factory.ChainedFactoryConstructor;
import com.amazon.crud4dynamo.extension.factory.ChainedMethodFactoryConfig;
import com.amazon.crud4dynamo.internal.factory.DefaultMethodFactory;
import com.amazon.crud4dynamo.internal.factory.ThrowingMethodFactory;
import com.amazon.crud4dynamo.internal.factory.TransactionGetMethodFactory;
import com.amazon.crud4dynamo.internal.factory.TransactionWriteMethodFactory;
import java.util.Arrays;
import java.util.List;
import lombok.Getter;

public enum DefaultTransactionFactoryConfig
    implements ChainedMethodFactoryConfig<DefaultTransactionFactoryConfig> {
  DEFAULT_METHOD(10000, DefaultMethodFactory::new),
  TRANSACTION_WRITE_METHOD(20000, TransactionWriteMethodFactory::new),
  TRANSACTION_GET_METHOD(30000, TransactionGetMethodFactory::new),
  THROWING_METHOD(Integer.MAX_VALUE, ThrowingMethodFactory::new);

  @Getter private final int order;
  @Getter private final ChainedFactoryConstructor chainedFactoryConstructor;

  DefaultTransactionFactoryConfig(
      final int order, final ChainedFactoryConstructor chainedFactoryConstructor) {
    this.order = order;
    this.chainedFactoryConstructor = chainedFactoryConstructor;
  }

  public static List<ChainedMethodFactoryConfig> getConfigs() {
    return Arrays.asList(values());
  }
}
