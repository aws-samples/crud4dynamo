package com.amazon.crud4dynamo.internal;

import com.amazon.crud4dynamo.Config;
import com.amazon.crud4dynamo.CrudForDynamo;
import com.amazon.crud4dynamo.crudinterface.SimpleKeyCrud;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.factory.ChainedFactoryConstructor;
import com.amazon.crud4dynamo.extension.factory.ChainedMethodFactoryConfig;
import com.amazon.crud4dynamo.internal.factory.MapperConfigAwareMethodFactory;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;

class OverrideConfigTest {

  private static class FactoryConfig implements ChainedMethodFactoryConfig<FactoryConfig> {
    private final ChainedFactoryConstructor constructor;

    private FactoryConfig(final ChainedFactoryConstructor constructor) {
      this.constructor = constructor;
    }

    @Override
    public int getOrder() {
      return 1;
    }

    @Override
    public ChainedFactoryConstructor getChainedFactoryConstructor() {
      return constructor;
    }
  }

  private static class IdentityChainedFactoryConstructor implements ChainedFactoryConstructor {
    private AbstractMethodFactory factory;

    @Override
    public AbstractMethodFactory apply(final AbstractMethodFactory abstractMethodFactory) {
      factory = abstractMethodFactory;
      return abstractMethodFactory;
    }
  }

  @Nested
  class WithEmptyConfig extends SimpleKeyCrudTest {
    @Override
    protected SimpleKeyCrud newDao() {
      return new CrudForDynamo(getDynamoDbClient(), Config.builder().build())
          .createSimple(getModelClass());
    }
  }

  @Nested
  class WithCustomMapperConfig extends SimpleKeyCrudTest {
    @Override
    protected SimpleKeyCrud newDao() {
      return new CrudForDynamo(
              getDynamoDbClient(),
              Config.builder()
                  .mapperConfig(
                      DynamoDBMapperConfig.builder()
                          .withConsistentReads(DynamoDBMapperConfig.ConsistentReads.CONSISTENT)
                          .build())
                  .build())
          .createSimple(getModelClass());
    }
  }

  @Nested
  class WithCustomFactory extends SimpleKeyCrudTest {
    private final IdentityChainedFactoryConstructor identityChainedFactoryConstructor =
        new IdentityChainedFactoryConstructor();

    @Override
    @BeforeEach
    public void setUp() throws Exception {
      super.setUp();
      AssertionsForClassTypes.assertThat(identityChainedFactoryConstructor.factory)
          .isInstanceOf(MapperConfigAwareMethodFactory.class);
    }

    @Override
    protected SimpleKeyCrud newDao() {
      return new CrudForDynamo(
              getDynamoDbClient(),
              Config.builder()
                  .crudFactoryConstructorConfig(
                      new FactoryConfig(identityChainedFactoryConstructor))
                  .build())
          .createSimple(getModelClass());
    }
  }
}
