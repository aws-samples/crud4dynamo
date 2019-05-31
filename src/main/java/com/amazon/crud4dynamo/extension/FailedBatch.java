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

package com.amazon.crud4dynamo.extension;

import java.util.List;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

/**
 * This class aggregates failures of a batch operation.
 *
 * @param <M> Model Generic Type Parameter
 */
@Builder
@Value
public class FailedBatch<M> {
  @Singular private final List<SubBatch<M>> subBatches;

  public boolean isEmpty() {
    return subBatches.isEmpty();
  }

  public List<M> getFailedItems() {
    return subBatches.stream()
        .map(SubBatch::getFailedItems)
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }

  /**
   * This class represent a sub-batch failure of a batch operations.
   *
   * @param <M> Model Generic Type Parameter
   */
  @Builder
  @Value
  public static class SubBatch<M> {
    private final List<M> failedItems;
    private final Exception exception;
  }
}
