package com.amazon.crud4dynamo.utility;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazon.crud4dynamo.extension.PageRequest;
import com.amazon.crud4dynamo.extension.PageResult;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.junit.jupiter.api.Test;

class PageResultCollectorTest {

  @Test
  void collect() {
    final PageRequestMocker<String> mocker = creatPageMocker();

    assertThat(newCollector(mocker).get()).containsAll(mocker.getAllItems());
  }

  private PageRequestMocker<String> creatPageMocker() {
    return new PageRequestMocker<String>().mockOnePage("A", "B").mockOnePage("C").finish();
  }

  private PageResultCollector newCollector(final PageRequestMocker<String> mocker) {
    return PageResultCollector.newCollector(mocker.getFirstRequest(), mocker.getRequester());
  }

  private static class PageRequestMocker<T> {
    private static final int DUMMY_MAX_PAGE_SIZE = 100;
    @Getter private final PageResultCollector.Requester requester;
    @Getter private PageRequest<T> firstRequest;
    private T lastEvaluatedItem = null;
    @Getter private final List<T> allItems = new ArrayList<>();

    private PageRequestMocker() {
      requester = mock(PageResultCollector.Requester.class);
    }

    public PageRequestMocker<T> mockOnePage(final T... obj) {
      Preconditions.checkArgument(obj.length >= 1, "At least contains one argument");
      final PageRequest<T> request =
          PageRequest.<T>builder()
              .limit(DUMMY_MAX_PAGE_SIZE)
              .exclusiveStartItem(lastEvaluatedItem)
              .build();
      if (firstRequest == null) {
        firstRequest = request;
      }

      final List<T> items = Arrays.stream(obj).collect(Collectors.toList());
      allItems.addAll(items);
      final PageResult<T> result =
          PageResult.<T>builder().items(items).lastEvaluatedItem(obj[obj.length - 1]).build();
      when(requester.issue(request)).thenReturn(result);
      lastEvaluatedItem = result.getLastEvaluatedItem();
      return this;
    }

    public PageRequestMocker<T> finish() {
      final PageRequest<T> request =
          PageRequest.<T>builder()
              .limit(DUMMY_MAX_PAGE_SIZE)
              .exclusiveStartItem(lastEvaluatedItem)
              .build();
      when(requester.issue(request))
          .thenReturn(
              PageResult.<T>builder().lastEvaluatedItem(null).items(new ArrayList<>()).build());
      return this;
    }
  }
}
