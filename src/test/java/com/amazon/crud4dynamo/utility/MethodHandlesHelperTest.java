package com.amazon.crud4dynamo.utility;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.invoke.MethodHandles.Lookup;
import org.junit.jupiter.api.Test;

/** TODO: add more test cases */
class MethodHandlesHelperTest {

  @Test
  void canCreateMethodLookup() {
    final Lookup lookup = MethodHandlesHelper.getLookup(String.class);

    assertThat(lookup).isNotNull();
  }
}
