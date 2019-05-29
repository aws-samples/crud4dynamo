package com.amazon.crud4dynamo.internal.method.query;

import com.amazon.crud4dynamo.extension.PageRequest;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PagingExpressionFactoryTest extends SingleTableDynamoDbTestBase<PagingExpressionFactoryTest.Model> {

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @DynamoDBTable(tableName = "Model")
    public static class Model {
        @DynamoDBHashKey(attributeName = "HashKey")
        private String hashKey;

        @DynamoDBRangeKey(attributeName = "RangeKey")
        private Integer rangeKey;
    }

    @Override
    protected Class<Model> getModelClass() {
        return Model.class;
    }

    @Test
    void create() {
        final QueryExpressionFactory mockFactory = getMockExpressionFactory();
        final PagingExpressionFactory factory = new PagingExpressionFactory(mockFactory, Model.class, getDynamoDbMapper());
        final Model model = Model.builder().hashKey("hashKey").rangeKey(1).build();
        final int limit = 10;
        final PageRequest<Model> pageRequest = PageRequest.<Model>builder().exclusiveStartItem(model).limit(limit).build();

        final DynamoDBQueryExpression expression = factory.create(pageRequest);

        AssertionsForClassTypes.assertThat(expression.getLimit()).isEqualTo(limit);
        assertThat(expression.getExclusiveStartKey()).isEqualTo(getDynamoDbMapperTableModel().convert(model));
    }

    private static QueryExpressionFactory getMockExpressionFactory() {
        final QueryExpressionFactory mockFactory = mock(QueryExpressionFactory.class);
        when(mockFactory.create(any())).thenReturn(new DynamoDBQueryExpression());
        return mockFactory;
    }
}
