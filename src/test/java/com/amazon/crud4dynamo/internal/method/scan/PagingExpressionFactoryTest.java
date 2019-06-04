package com.amazon.crud4dynamo.internal.method.scan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazon.crud4dynamo.extension.PageRequest;
import com.amazon.crud4dynamo.internal.method.scan.PagingExpressionFactoryTest.Model;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;

class PagingExpressionFactoryTest extends SingleTableDynamoDbTestBase<Model> {
  @Override
  protected Class<Model> getModelClass() {
    return Model.class;
  }

  @Test
  void create() {
    final PagingExpressionFactory factory = getFactory();
    final Model model = Model.builder().hashKey("hashKey").rangeKey(1).build();
    final int limit = 10;
    final PageRequest<Model> pageRequest =
        PageRequest.<Model>builder().exclusiveStartItem(model).limit(limit).build();

    final DynamoDBScanExpression expression = factory.create(pageRequest);

    assertThat(expression.getLimit()).isEqualTo(limit);
    assertThat(expression.getExclusiveStartKey())
        .isEqualTo(getDynamoDbMapperTableModel().convert(model));
  }

  private PagingExpressionFactory getFactory() {
    final ScanExpressionFactory mockFactory = mock(ScanExpressionFactory.class);
    when(mockFactory.create(any())).thenReturn(new DynamoDBScanExpression());
    return new PagingExpressionFactory(mockFactory, Model.class, getDynamoDbMapper());
  }

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
}
