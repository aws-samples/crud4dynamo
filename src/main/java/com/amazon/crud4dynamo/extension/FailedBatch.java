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
        return subBatches.stream().map(SubBatch::getFailedItems).flatMap(List::stream).collect(Collectors.toList());
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
