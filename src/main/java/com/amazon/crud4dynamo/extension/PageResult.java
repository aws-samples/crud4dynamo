package com.amazon.crud4dynamo.extension;

import java.util.List;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

@Value
@Builder
public class PageResult<T> {
    @Singular private List<T> items;
    private T lastEvaluatedItem;
}
