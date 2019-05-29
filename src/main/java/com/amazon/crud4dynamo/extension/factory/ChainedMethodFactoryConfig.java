package com.amazon.crud4dynamo.extension.factory;

import lombok.NonNull;

public interface ChainedMethodFactoryConfig<T extends ChainedMethodFactoryConfig> extends Comparable<T> {
    int getOrder();

    ChainedFactoryConstructor getChainedFactoryConstructor();

    @Override
    default int compareTo(@NonNull final T o) {
        return Integer.compare(getOrder(), o.getOrder());
    }
}
