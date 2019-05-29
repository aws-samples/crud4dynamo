package com.amazon.crud4dynamo.extension;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class PageRequest<T> {
    private final Integer limit;
    private final T exclusiveStartItem;
}
