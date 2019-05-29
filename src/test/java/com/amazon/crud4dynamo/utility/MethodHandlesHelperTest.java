package com.amazon.crud4dynamo.utility;

import java.lang.invoke.MethodHandles.Lookup;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** TODO: add more test cases */
class MethodHandlesHelperTest {

    @Test
    void canCreateMethodLookup() {
        final Lookup lookup = MethodHandlesHelper.getLookup(String.class);

        assertThat(lookup).isNotNull();
    }
}
