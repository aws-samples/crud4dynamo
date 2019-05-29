package com.amazon.crud4dynamo.internal.method;

import com.amazon.crud4dynamo.annotation.Cached;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import lombok.EqualsAndHashCode;
import lombok.NonNull;

public class CachedMethod implements AbstractMethod {
    private final Signature signature;
    private final AbstractMethod delegate;
    private final LoadingCache<ArgumentsWrapper, Object> cache;

    public CachedMethod(@NonNull final Signature signature, @NonNull final AbstractMethod delegate) {
        this.signature = signature;
        this.delegate = delegate;
        cache = newCache();
    }

    private static CacheBuilder<Object, Object> setExpireAfterWrite(final CacheBuilder<Object, Object> builder, final Cached cacheConfig) {
        return cacheConfig.expireAfterWrite() < 0
                ? builder
                : builder.expireAfterWrite(cacheConfig.expireAfterWrite(), cacheConfig.expireAfterWriteTimeUnit());
    }

    private static CacheBuilder<Object, Object> setExpireAfterAccess(final CacheBuilder<Object, Object> builder, final Cached cacheConfig) {
        return cacheConfig.expireAfterAccess() < 0
                ? builder
                : builder.expireAfterAccess(cacheConfig.expireAfterAccess(), cacheConfig.expireAfterAccessTimeUnit());
    }

    private LoadingCache<ArgumentsWrapper, Object> newCache() {
        final Cached cacheConfig = signature.invokable().getAnnotation(Cached.class);
        CacheBuilder<Object, Object> builder =
                CacheBuilder.newBuilder()
                        .initialCapacity(cacheConfig.initialCapacity())
                        .maximumSize(cacheConfig.maxSize())
                        .concurrencyLevel(cacheConfig.concurrencyLevel());
        builder = setExpireAfterAccess(builder, cacheConfig);
        builder = setExpireAfterWrite(builder, cacheConfig);
        return builder.build(
                new CacheLoader<ArgumentsWrapper, Object>() {
                    @Override
                    public Object load(final ArgumentsWrapper wrapper) {
                        try {
                            return delegate.invoke(wrapper.args);
                        } catch (final Throwable throwable) {
                            throw new RuntimeException("Failed to load cache value", throwable);
                        }
                    }
                });
    }

    @Override
    public Signature getSignature() {
        return signature;
    }

    @Override
    public Object invoke(final Object... args) throws Throwable {
        return cache.get(new ArgumentsWrapper(args));
    }

    @Override
    public AbstractMethod bind(final Object target) {
        delegate.bind(target);
        return this;
    }

    @EqualsAndHashCode
    private static class ArgumentsWrapper {
        private final Object[] args;

        private ArgumentsWrapper(final Object... args) {
            this.args = args;
        }
    }
}
