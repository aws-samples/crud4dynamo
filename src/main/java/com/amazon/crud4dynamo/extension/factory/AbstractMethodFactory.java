package com.amazon.crud4dynamo.extension.factory;

import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;

public interface AbstractMethodFactory {
    AbstractMethod create(final Context context);
}
