package com.amazon.crud4dynamo.utility;

import java.io.IOException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class ExceptionHelperTest {

  @Test
  public void throwDirectly() {
    Assertions.assertThatThrownBy(this::methodThrowCheckedExceptionAsUnchecked)
        .isExactlyInstanceOf(IOException.class)
        .hasCause(null);
  }

  private void methodThrowCheckedExceptionAsUnchecked() {
    try {
      methodThrowCheckedException();
    } catch (final IOException e) {
      throw ExceptionHelper.throwAsUnchecked(e);
    }
  }

  private void methodThrowCheckedException() throws IOException {
    throw new IOException();
  }
}
