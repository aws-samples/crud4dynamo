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

public enum DefaultTransactionFactoryConfig implements ChainedMethodFactoryConfig<DefaultTransactionFactoryConfig> {
    DEFAULT_METHOD(10000, DefaultMethodFactory::new),
    TRANSACTION_WRITE_METHOD(20000, TransactionWriteMethodFactory::new),
    TRANSACTION_GET_METHOD(30000, TransactionGetMethodFactory::new),
    THROWING_METHOD(Integer.MAX_VALUE, ThrowingMethodFactory::new);

    @Getter
    private final int order;
    @Getter
    private final ChainedFactoryConstructor chainedFactoryConstructor;

    DefaultTransactionFactoryConfig(final int order, final ChainedFactoryConstructor chainedFactoryConstructor) {
        this.order = order;
        this.chainedFactoryConstructor = chainedFactoryConstructor;
    }

    public static List<ChainedMethodFactoryConfig> getConfigs() {
        return Arrays.asList(values());
    }
}
