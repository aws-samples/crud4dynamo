package com.amazon.crud4dynamo.internal.utility;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.PaginationLoadingStrategy;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DynamoDbMapperConfigHelperTest {

    @Test
    void verifyOverrideConfig() {
        final DynamoDBMapperConfig baseConfig = DynamoDBMapperConfig.builder().build();

        assertThat(baseConfig.getPaginationLoadingStrategy()).isNull();

        final DynamoDBMapperConfig overriddenConfig =
                DynamoDbMapperConfigHelper.override(baseConfig, PaginationLoadingStrategy.LAZY_LOADING.config());

        assertThat(overriddenConfig.getPaginationLoadingStrategy()).isEqualTo(PaginationLoadingStrategy.LAZY_LOADING);
    }
}
