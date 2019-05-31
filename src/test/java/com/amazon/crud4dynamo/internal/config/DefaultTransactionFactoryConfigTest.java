package com.amazon.crud4dynamo.internal.config;

import com.amazon.crud4dynamo.extension.factory.ChainedMethodFactoryConfig;
import java.util.stream.Collectors;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.Test;

class DefaultTransactionFactoryConfigTest {
  @Test
  void order_should_be_unique() {
    final int numberOfUniqueConfigs =
        DefaultTransactionFactoryConfig.getConfigs().stream()
            .map(ChainedMethodFactoryConfig::getOrder)
            .collect(Collectors.toSet())
            .size();

    AssertionsForClassTypes.assertThat(DefaultTransactionFactoryConfig.getConfigs().size())
        .isEqualTo(numberOfUniqueConfigs);
  }
}
