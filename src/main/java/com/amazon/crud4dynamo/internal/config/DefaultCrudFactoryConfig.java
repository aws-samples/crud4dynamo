package com.amazon.crud4dynamo.internal.config;

import com.amazon.crud4dynamo.extension.factory.ChainedFactoryConstructor;
import com.amazon.crud4dynamo.extension.factory.ChainedMethodFactoryConfig;
import com.amazon.crud4dynamo.internal.factory.BasicCrudMethodFactory;
import com.amazon.crud4dynamo.internal.factory.CachedMethodFactory;
import com.amazon.crud4dynamo.internal.factory.CustomMethodFactory;
import com.amazon.crud4dynamo.internal.factory.DefaultMethodFactory;
import com.amazon.crud4dynamo.internal.factory.DeleteMethodFactory;
import com.amazon.crud4dynamo.internal.factory.MapperConfigAwareMethodFactory;
import com.amazon.crud4dynamo.internal.factory.ParallelScanMethodFactory;
import com.amazon.crud4dynamo.internal.factory.PutMethodFactory;
import com.amazon.crud4dynamo.internal.factory.QueryMethodFactory;
import com.amazon.crud4dynamo.internal.factory.ScanMethodFactory;
import com.amazon.crud4dynamo.internal.factory.ThrowingMethodFactory;
import com.amazon.crud4dynamo.internal.factory.UpdateMethodFactory;
import java.util.Arrays;
import java.util.List;
import lombok.Getter;

public enum DefaultCrudFactoryConfig implements ChainedMethodFactoryConfig<DefaultCrudFactoryConfig> {
    MAPPER_CONFIG_AWARE_METHOD(10000, MapperConfigAwareMethodFactory::new),
    CACHED_METHOD(20000, CachedMethodFactory::new),
    DEFAULT_METHOD(30000, DefaultMethodFactory::new),
    CUSTOM_METHOD(40000, CustomMethodFactory::new),
    QUERY_METHOD(50000, QueryMethodFactory::new),
    PARALLEL_SCAN_METHOD(60000, ParallelScanMethodFactory::new),
    SCAN_METHOD(70000, ScanMethodFactory::new),
    UPDATE_METHOD(80000, UpdateMethodFactory::new),
    PUT_METHOD(90000, PutMethodFactory::new),
    DELETE_METHOD(100000, DeleteMethodFactory::new),
    BASIC_CRUD_METHOD(110000, BasicCrudMethodFactory::new),
    THROWING_METHOD(Integer.MAX_VALUE, ThrowingMethodFactory::new);

    @Getter
    private final int order;
    @Getter
    private final ChainedFactoryConstructor chainedFactoryConstructor;

    DefaultCrudFactoryConfig(final int order, final ChainedFactoryConstructor chainedFactoryConstructor) {
        this.order = order;
        this.chainedFactoryConstructor = chainedFactoryConstructor;
    }

    public static List<ChainedMethodFactoryConfig<?>> getConfigs() {
        return Arrays.asList(values());
    }
}
