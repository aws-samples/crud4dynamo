/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.crud4dynamo.utility;

import com.amazon.crud4dynamo.extension.PageRequest;
import com.amazon.crud4dynamo.extension.PageResult;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PageResultCollector<T, R> {
    private final PageRequest<T> initialRequest;
    private final Requester<T> requester;
    private final Function<T, R> resultMapper;

    public PageResultCollector(final PageRequest<T> initialRequest, final Requester<T> requester, final Function<T, R> resultMapper) {
        this.initialRequest = initialRequest;
        this.requester = requester;
        this.resultMapper = resultMapper;
    }

    public static <E> PageResultCollector<E, E> newCollector(final PageRequest<E> initialRequest, final Requester<E> requester) {
        return new PageResultCollector<>(initialRequest, requester, Function.identity());
    }

    public List<R> get() {
        final List<R> ret = new ArrayList<>();
        PageRequest<T> request = initialRequest;
        T lastEvaluatedItem = null;
        do {
            final PageResult<T> result = requester.issue(request);
            ret.addAll(result.getItems().stream().map(resultMapper).collect(Collectors.toList()));
            lastEvaluatedItem = result.getLastEvaluatedItem();
            request = rebuildRequest(request, lastEvaluatedItem);
        } while (lastEvaluatedItem != null);
        return ret;
    }

    private PageRequest<T> rebuildRequest(final PageRequest<T> request, final T lastEvaluatedItem) {
        return PageRequest.<T>builder().limit(request.getLimit()).exclusiveStartItem(lastEvaluatedItem).build();
    }

    public interface Requester<T> {
        PageResult<T> issue(final PageRequest<T> request);
    }
}
