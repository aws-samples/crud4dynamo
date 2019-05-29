package com.amazon.crud4dynamo.extension.factory;

import lombok.Getter;

public class FactoryConfig implements ChainedMethodFactoryConfig<FactoryConfig> {
    @Getter private final int order;
    @Getter private final ChainedFactoryConstructor chainedFactoryConstructor;

    public FactoryConfig(final int order, final ChainedFactoryConstructor chainedFactoryConstructor) {
        this.order = order;
        this.chainedFactoryConstructor = chainedFactoryConstructor;
    }
}
