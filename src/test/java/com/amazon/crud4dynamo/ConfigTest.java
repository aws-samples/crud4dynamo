package com.amazon.crud4dynamo;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.Mockito.mock;

import com.amazon.crud4dynamo.extension.factory.ChainedFactoryConstructor;
import com.amazon.crud4dynamo.extension.factory.ChainedMethodFactoryConfig;
import com.amazon.crud4dynamo.extension.factory.FactoryConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.Test;

class ConfigTest {

  @Test
  void oneConfigIsNull_returnTheOther() {
    final Config nullConfig = null;
    final Config notNullConfig = Config.builder().build();

    final Config mergedConfig = Config.merge(nullConfig, notNullConfig);

    AssertionsForClassTypes.assertThat(mergedConfig).isEqualTo(notNullConfig);
  }

  @Test
  void mergeMapperConfig() {
    final Config config1 = Config.builder().mapperConfig(null).build();
    final Config config2 = Config.builder().mapperConfig(DynamoDBMapperConfig.DEFAULT).build();

    final Config mergedConfig = Config.merge(config1, config2);

    AssertionsForClassTypes.assertThat(mergedConfig.mapperConfig())
        .isEqualTo(DynamoDBMapperConfig.DEFAULT);
    assertThat(mergedConfig.crudFactoryConstructorConfigs()).isEmpty();
  }

  @Test
  void mergeFactoryConstructors() {
    final ChainedMethodFactoryConfig config1 = mockChainedMethodFactoryConfig(1);
    final ChainedMethodFactoryConfig config2 = mockChainedMethodFactoryConfig(2);
    final ChainedMethodFactoryConfig config3 = mockChainedMethodFactoryConfig(2);

    final Config base =
        Config.builder()
            .crudFactoryConstructorConfig(config1)
            .crudFactoryConstructorConfig(config3)
            .build();
    final Config overrides = Config.builder().crudFactoryConstructorConfig(config2).build();

    final Config mergedConfig = Config.merge(base, overrides);

    assertThat(mergedConfig.crudFactoryConstructorConfigs()).containsExactly(config1, config2);
  }

  @Test
  void mergeTransactionFactoryConstructors() {
    final ChainedMethodFactoryConfig config1 = mockChainedMethodFactoryConfig(1);
    final ChainedMethodFactoryConfig config2 = mockChainedMethodFactoryConfig(2);
    final ChainedMethodFactoryConfig config3 = mockChainedMethodFactoryConfig(2);

    final Config base =
        Config.builder()
            .transactionFactoryConstructorConfig(config1)
            .transactionFactoryConstructorConfig(config3)
            .build();
    final Config overrides = Config.builder().transactionFactoryConstructorConfig(config2).build();

    final Config mergedConfig = Config.merge(base, overrides);

    assertThat(mergedConfig.transactionFactoryConstructorConfigs())
        .containsExactly(config1, config2);
  }

  private ChainedMethodFactoryConfig mockChainedMethodFactoryConfig(final int order) {
    return new FactoryConfig(order, mock(ChainedFactoryConstructor.class));
  }
}
