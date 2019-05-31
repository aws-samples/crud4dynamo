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
package com.amazon.crud4dynamo;

import com.amazon.crud4dynamo.extension.factory.ChainedMethodFactoryConfig;
import com.amazon.crud4dynamo.internal.utility.DynamoDbMapperConfigHelper;
import com.amazon.crud4dynamo.utility.MapHelper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Builder
@Accessors(fluent = true, chain = true)
public class Config {

  private DynamoDBMapperConfig mapperConfig;
  @Singular private List<ChainedMethodFactoryConfig> crudFactoryConstructorConfigs;
  @Singular private List<ChainedMethodFactoryConfig> transactionFactoryConstructorConfigs;

  public static Config merge(final Config base, final Config overrides) {
    if (null == base || null == overrides) {
      return Optional.ofNullable(base).orElse(overrides);
    }
    return Config.builder()
        .mapperConfig(
            DynamoDbMapperConfigHelper.override(base.mapperConfig, overrides.mapperConfig))
        .crudFactoryConstructorConfigs(
            mergeAndSort(
                base.crudFactoryConstructorConfigs, overrides.crudFactoryConstructorConfigs))
        .transactionFactoryConstructorConfigs(
            mergeAndSort(
                base.transactionFactoryConstructorConfigs,
                overrides.transactionFactoryConstructorConfigs))
        .build();
  }

  private static List<ChainedMethodFactoryConfig> mergeAndSort(
      final List<ChainedMethodFactoryConfig> base,
      final List<ChainedMethodFactoryConfig> overrides) {
    return MapHelper.overrideMerge(toMap(base), toMap(overrides)).values().stream()
        .sorted()
        .collect(Collectors.toList());
  }

  private static Map<Integer, ChainedMethodFactoryConfig> toMap(
      final List<ChainedMethodFactoryConfig> list) {
    return list.stream()
        .collect(Collectors.toMap(ChainedMethodFactoryConfig::getOrder, Function.identity()));
  }
}
