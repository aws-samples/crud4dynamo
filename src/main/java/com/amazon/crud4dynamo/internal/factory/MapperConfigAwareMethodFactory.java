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

import com.amazon.crud4dynamo.annotation.MapperConfig;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.Context.ContextBuilder;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.factory.ChainedAbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.internal.utility.DynamoDbMapperConfigHelper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import java.util.Optional;
import java.util.function.Function;

public class MapperConfigAwareMethodFactory extends ChainedAbstractMethodFactory {
  public MapperConfigAwareMethodFactory(final AbstractMethodFactory delegate) {
    super(delegate);
  }

  private static Optional<MapperConfig> getAnnotation(Context context) {
    return context.signature().getAnnotation(MapperConfig.class);
  }

  private static Function<MapperConfig, Context> overrideConfigInContext(Context context) {
    return annotation ->
        newBuilder(context).mapperConfig(overrideMapperConfig(context, annotation)).build();
  }

  private static DynamoDBMapperConfig overrideMapperConfig(
      Context context, MapperConfig annotation) {
    return DynamoDbMapperConfigHelper.override(
        context.mapperConfig(),
        DynamoDBMapperConfig.builder()
            .withConsistentReads(annotation.consistentReads())
            .withSaveBehavior(annotation.saveBehavior())
            .build());
  }

  private static ContextBuilder newBuilder(Context context) {
    return Context.builder()
        .method(context.method())
        .signature(context.signature())
        .modelType(context.modelType())
        .interfaceType(context.interfaceType())
        .mapper(context.mapper())
        .amazonDynamoDb(context.amazonDynamoDb());
  }

  @Override
  public AbstractMethod create(final Context context) {
    return super.create(
        getAnnotation(context).map(overrideConfigInContext(context)).orElse(context));
  }
}
