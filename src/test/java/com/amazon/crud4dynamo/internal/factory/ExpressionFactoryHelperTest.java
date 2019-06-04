package com.amazon.crud4dynamo.internal.factory;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.extension.Argument;
import com.amazon.crud4dynamo.extension.PageRequest;
import com.amazon.crud4dynamo.internal.parsing.AttributeValueMapper;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazon.crud4dynamo.utility.MapHelper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.Invokable;
import com.google.common.reflect.Parameter;
import com.google.common.reflect.TypeToken;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.assertj.core.data.MapEntry;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ExpressionFactoryHelperTest {

  @Test
  void mapIsEmpty_returnNull() {
    final Map<String, String> emptyMap = Collections.EMPTY_MAP;

    final Map<String, String> returnedMap = MapHelper.toNullIfEmpty(emptyMap);

    assertThat(returnedMap).isNull();
  }

  @Test
  void mapIsNotEmpty_returnOriginal() {
    final Map<String, String> nonEmptyMap = ImmutableMap.of("A", "B");

    final Map<String, String> returnedMap = MapHelper.toNullIfEmpty(nonEmptyMap);

    assertThat(returnedMap == nonEmptyMap).isTrue();
  }

  @Test
  void stringIsNull_returnNull() {
    final String nullStr = null;

    final String returnedStr = ExpressionFactoryHelper.toNullIfBlank(nullStr);

    assertThat(returnedStr).isNull();
  }

  @Test
  void stringIsEmpty_returnNull() {
    final String emptyStr = "";

    final String returnedStr = ExpressionFactoryHelper.toNullIfBlank(emptyStr);

    assertThat(returnedStr).isNull();
  }

  @Test
  void stringIsNotEmpty_returnOriginal() {
    final String nonEmptyStr = "a";

    final String returnedStr = ExpressionFactoryHelper.toNullIfBlank(nonEmptyStr);

    assertThat(returnedStr == nonEmptyStr).isTrue();
  }

  @Test
  void getAttributeNames() throws Exception {
    final ImmutableList<Parameter> parameters = TestInterface.getMethodParameters();
    final List<Argument> arguments = Argument.newList(parameters, Arrays.asList("AttrName", 1));

    final Map<String, String> exprAttrNames =
        ExpressionFactoryHelper.getExpressionAttributeNames(arguments);

    assertThat(exprAttrNames).containsOnly(MapEntry.entry("#AttrName", "AttrName"));
  }

  @Test
  void getFilteredAttributeNames() throws Exception {
    final ImmutableList<Parameter> parameters = TestInterface.getMethodParameters();
    final List<Argument> arguments = Argument.newList(parameters, Arrays.asList("AttrName", 1));

    final Map<String, String> exprAttrNames =
        ExpressionFactoryHelper.getExpressionAttributeNames(arguments, value -> false);

    assertThat(exprAttrNames).isEmpty();
  }

  @Test
  void getAttributeValues() throws Exception {
    final ImmutableList<Parameter> parameters = TestInterface.getMethodParameters();
    final List<Argument> arguments = Argument.newList(parameters, Arrays.asList("AttrName", 1));
    final AttributeValueMapper mapper =
        new AttributeValueMapper(
            ImmutableMap.of(":attrValue", object -> new AttributeValue().withN(object.toString())));

    final Map<String, AttributeValue> exprAttrValues =
        ExpressionFactoryHelper.getExpressionAttributeValues(arguments, mapper);

    assertThat(exprAttrValues)
        .containsOnly(MapEntry.entry(":attrValue", new AttributeValue().withN("1")));
  }

  @Test
  void hasPageRequest() {
    final PageRequest<Object> request = PageRequest.builder().build();

    final Optional<PageRequest> pageRequest =
        ExpressionFactoryHelper.findPageRequest(new Object(), request);

    assertThat(pageRequest).contains(request);
  }

  @Test
  void noPageRequest() {
    final Optional<PageRequest> pageRequest = ExpressionFactoryHelper.findPageRequest(new Object());

    assertThat(pageRequest).isEmpty();
  }

  @Test
  void getTableName() {
    final String tableName = ExpressionFactoryHelper.getTableName(Model.class);

    assertThat(tableName).isEqualTo("Table");
  }

  public interface TestInterface {
    static ImmutableList<Parameter> getMethodParameters() throws NoSuchMethodException {
      final Method aMethod = TestInterface.class.getMethod("aMethod", String.class, Integer.class);
      final TypeToken<TestInterface> typeToken = TypeToken.of(TestInterface.class);
      final Invokable<TestInterface, Object> invokable = typeToken.method(aMethod);
      return invokable.getParameters();
    }

    void aMethod(@Param("#AttrName") String attrName, @Param(":attrValue") Integer attrValue);
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @DynamoDBTable(tableName = "Table")
  public static class Model {
    @DynamoDBHashKey(attributeName = "HashKey")
    private String hashKey;
  }

  @Nested
  class MapperNeededTest extends SingleTableDynamoDbTestBase<Model> {
    @Override
    protected Class<Model> getModelClass() {
      return Model.class;
    }

    @Test
    void getLastEvaluatedKey() {
      final PageRequest<Model> request =
          PageRequest.<Model>builder()
              .exclusiveStartItem(Model.builder().hashKey("A").build())
              .build();

      final Map<String, AttributeValue> lastEvaluatedKey =
          ExpressionFactoryHelper.getLastEvaluatedKey(request, getDynamoDbMapperTableModel());

      assertThat(lastEvaluatedKey).containsOnly(MapEntry.entry("HashKey", new AttributeValue("A")));
    }
  }
}
