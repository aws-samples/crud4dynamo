package com.amazon.crud4dynamo.extension.factory;

import java.util.function.Function;

public interface ChainedFactoryConstructor extends Function<AbstractMethodFactory, AbstractMethodFactory> {}
