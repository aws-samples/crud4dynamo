package com.amazon.crud4dynamo.internal.method;

import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.annotation.Update;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.internal.method.UpdateMethodTest.Model;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UpdateMethodTest extends SingleTableDynamoDbTestBase<Model> {
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

        @DynamoDBAttribute(attributeName = "Integer1")
        private Integer integer1;

        @DynamoDBAttribute(attributeName = "StrList1")
        private List<String> strList1;

        @DynamoDBAttribute(attributeName = "StrMap1")
        private Map<String, Map<String, String>> strMap1;

        @DynamoDBAttribute(attributeName = "StrSet1")
        private Set<String> strSet1;
    }

    @Override
    protected Class<Model> getModelClass() {
        return Model.class;
    }

    interface Dao {
        @Update(keyExpression = "HashKey = :hashKey, #rangeKey = :rangeKey", updateExpression = "SET #attr = #rangeKey")
        Model set_base_case1(
                @Param(":hashKey") final String hashKeyValue,
                @Param("#rangeKey") final String rangeKeyName,
                @Param(":rangeKey") final String rangeKeyValue,
                @Param("#attr") final String targetAttrName);

        @Update(
                keyExpression = "HashKey = :hashKey, #rangeKey = :rangeKey",
                updateExpression = "SET #attr = #rangeKey",
                returnValue = ReturnValue.ALL_NEW)
        Model set_base_case2(
                @Param(":hashKey") final String hashKeyValue,
                @Param("#rangeKey") final String rangeKeyName,
                @Param(":rangeKey") final String rangeKeyValue,
                @Param("#attr") final String targetAttrName);

        @Update(
                keyExpression = "HashKey = :hashKey, RangeKey = :rangeKey",
                updateExpression = "SET Integer1 = if_not_exists(Integer1, :val)",
                returnValue = ReturnValue.ALL_NEW)
        Model set_if_not_exists(@Param(":hashKey") final String hashKey, @Param(":rangeKey") final String rangeKey, @Param(":val") int val);

        @Update(
                keyExpression = "HashKey = :hashKey, RangeKey = :rangeKey",
                updateExpression = "SET StrList1 = list_append(StrList1, :vals)",
                returnValue = ReturnValue.ALL_NEW)
        Model set_list_append(
                @Param(":hashKey") final String hashKey, @Param(":rangeKey") final String rangeKey, @Param(":vals") List<String> vals);

        @Update(
                keyExpression = "HashKey = :hashKey, RangeKey = :rangeKey",
                updateExpression = "SET StrList1 = list_append(StrList1, :vals)",
                conditionExpression = "Integer1 > :val",
                returnValue = ReturnValue.ALL_NEW)
        Model set_list_append_with_condition(
                @Param(":hashKey") final String hashKey,
                @Param(":rangeKey") final String rangeKey,
                @Param(":vals") List<String> vals,
                @Param(":val") int val);

        @Update(
                keyExpression = "HashKey = :hashKey, RangeKey = :rangeKey",
                updateExpression = "SET StrList1[0] = :val",
                returnValue = ReturnValue.ALL_NEW)
        Model set_add_element_to_a_list(
                @Param(":hashKey") final String hashKey, @Param(":rangeKey") final String rangeKey, @Param(":val") String val);

        @Update(
                keyExpression = "HashKey = :hashKey, RangeKey = :rangeKey",
                updateExpression = "SET #path1.#path2.#path3 = :val",
                returnValue = ReturnValue.ALL_NEW)
        Model set_adding_nested_map_attributes(
                @Param(":hashKey") final String hashKey,
                @Param(":rangeKey") final String rangeKey,
                @Param("#path1") String path1,
                @Param("#path2") String path2,
                @Param("#path3") String path3,
                @Param(":val") String value);

        @Update(
                keyExpression = "HashKey = :hashKey, RangeKey = :rangeKey",
                updateExpression = "SET StrMap1 = :val",
                returnValue = ReturnValue.ALL_NEW)
        Model set_adding_nested_map_attributes_2(
                @Param(":hashKey") final String hashKey, @Param(":rangeKey") final String rangeKey, @Param(":val") String value);

        @Update(
                keyExpression = "HashKey = :hashKey, RangeKey = :rangeKey",
                updateExpression = "SET StrMap1.A = :val",
                returnValue = ReturnValue.ALL_NEW)
        Model set_adding_nested_map(
                @Param(":hashKey") final String hashKey,
                @Param(":rangeKey") final String rangeKey,
                @Param(":val") Map<String, String> value);

        @Update(
                keyExpression = "HashKey = :hashKey, RangeKey = :rangeKey",
                updateExpression = "REMOVE #path",
                returnValue = ReturnValue.ALL_NEW)
        Model remove_attribute(
                @Param(":hashKey") final String hashKey, @Param(":rangeKey") final String rangeKey, @Param("#path") final String path);

        @Update(
                keyExpression = "HashKey = :hashKey, RangeKey = :rangeKey",
                updateExpression = "REMOVE StrList1[0], StrList1[2]",
                returnValue = ReturnValue.ALL_NEW)
        Model remove_elements_from_a_list(@Param(":hashKey") final String hashKey, @Param(":rangeKey") final String rangeKey);

        @Update(
                keyExpression = "HashKey = :hashKey, RangeKey = :rangeKey",
                updateExpression = "ADD Integer1 :val",
                returnValue = ReturnValue.ALL_NEW)
        Model add_a_number(
                @Param(":hashKey") final String hashKey, @Param(":rangeKey") final String rangeKey, @Param(":val") final int val);

        @Update(
                keyExpression = "HashKey = :hashKey, RangeKey = :rangeKey",
                updateExpression = "ADD StrSet1 :val",
                returnValue = ReturnValue.ALL_NEW)
        Model add_elements_to_a_set(
                @Param(":hashKey") final String hashKey, @Param(":rangeKey") final String rangeKey, @Param(":val") final Set<String> val);

        @Update(
                keyExpression = "HashKey = :hashKey, RangeKey = :rangeKey",
                updateExpression = "DELETE StrSet1 :val",
                returnValue = ReturnValue.ALL_NEW)
        Model delete_elements_to_a_set(
                @Param(":hashKey") final String hashKey, @Param(":rangeKey") final String rangeKey, @Param(":val") final Set<String> val);
    }

    @Test
    void set_base_case1() throws Throwable {
        final List<Model> models = storeItems(Stream.of(Model.builder().hashKey("A").rangeKey(1).build()));
        final Method method = Dao.class.getMethod("set_base_case1", String.class, String.class, String.class, String.class);

        final Object result = invoke(newUpdateMethod(method), "A", "RangeKey", 1, "Integer1");

        assertThat(result).isNull();
        assertThat(getItem(models.get(0))).contains(Model.builder().hashKey("A").rangeKey(1).integer1(1).build());
    }

    @Test
    void set_base_case2() throws Throwable {
        final List<Model> models = storeItems(Stream.of(Model.builder().hashKey("A").rangeKey(1).build()));
        final Method method = Dao.class.getMethod("set_base_case2", String.class, String.class, String.class, String.class);

        final Object result = invoke(newUpdateMethod(method), "A", "RangeKey", 1, "Integer1");

        final Model expected = Model.builder().hashKey("A").rangeKey(1).integer1(1).build();
        assertThat(result).isNotNull();
        verify((Model) result, expected, models.get(0));
    }

    @Test
    void set_using_if_not_exists() throws Throwable {
        storeItems(Stream.of(Model.builder().hashKey("A").rangeKey(1).build()));
        final Method method = Dao.class.getMethod("set_if_not_exists", String.class, String.class, int.class);

        final Model result = invoke(newUpdateMethod(method), "A", 1, 100);

        final Model expected = Model.builder().hashKey("A").rangeKey(1).integer1(100).build();
        verify(result, expected, expected);
    }

    @Test
    void set_using_list_append() throws Throwable {
        storeItems(Stream.of(Model.builder().hashKey("A").rangeKey(1).strList1(Collections.singletonList("Hello")).build()));
        final Method method = Dao.class.getMethod("set_list_append", String.class, String.class, List.class);

        final Model result = invoke(newUpdateMethod(method), "A", 1, Collections.singletonList("World"));

        final Model expected = Model.builder().hashKey("A").rangeKey(1).strList1(Arrays.asList("Hello", "World")).build();
        verify(result, expected, expected);
    }

    @Test
    void set_with_conditional_expression() throws Throwable {
        storeItems(Stream.of(Model.builder().hashKey("A").rangeKey(1).integer1(10).strList1(Collections.singletonList("A")).build()));
        final Method method = Dao.class.getMethod("set_list_append_with_condition", String.class, String.class, List.class, int.class);
        {
            final Model result = invoke(newUpdateMethod(method), "A", 1, Collections.singletonList("A"), 9);

            final Model expected = Model.builder().hashKey("A").rangeKey(1).integer1(10).strList1(Arrays.asList("A", "A")).build();
            verify(result, expected, expected);
        }

        {
            assertThatThrownBy(() -> invoke(newUpdateMethod(method), "A", 1, Collections.singletonList("A"), 10))
                    .isInstanceOf(ConditionalCheckFailedException.class);
        }
    }

    @Test
    void set_add_element_to_a_list() throws Throwable {
        storeItems(Stream.of(Model.builder().hashKey("A").rangeKey(1).integer1(10).strList1(Arrays.asList("A")).build()));
        final Method method = Dao.class.getMethod("set_add_element_to_a_list", String.class, String.class, String.class);

        final Model result = invoke(newUpdateMethod(method), "A", 1, "Hello");

        final Model expected = Model.builder().hashKey("A").rangeKey(1).integer1(10).strList1(Collections.singletonList("Hello")).build();
        verify(result, expected, expected);
    }

    @Test
    void set_adding_nested_map_attributes() throws Throwable {
        storeItems(Model.builder().hashKey("A").rangeKey(1).strMap1(ImmutableMap.of("A", ImmutableMap.of("B", "C"))).build());
        final Method method =
                Dao.class.getMethod(
                        "set_adding_nested_map_attributes",
                        String.class,
                        String.class,
                        String.class,
                        String.class,
                        String.class,
                        String.class);

        final Model result = invoke(newUpdateMethod(method), "A", 1, "StrMap1", "A", "C", "A");

        final Model expected =
                Model.builder().hashKey("A").rangeKey(1).strMap1(ImmutableMap.of("A", ImmutableMap.of("B", "C", "C", "A"))).build();
        verify(result, expected, expected);
    }

    private void verify(final Model result, final Model expected, final Model expected2) {
        assertThat(result).isEqualTo(expected);
        assertThat(getItem(expected2)).contains(expected);
    }

    @Test
    void set_adding_nested_map() throws Throwable {
        storeItems(Model.builder().hashKey("A").rangeKey(1).strMap1(ImmutableMap.of()).build());
        final Method method = Dao.class.getMethod("set_adding_nested_map", String.class, String.class, Map.class);

        final Model result = invoke(newUpdateMethod(method), "A", 1, ImmutableMap.of("A", "B"));

        final Model expected = Model.builder().hashKey("A").rangeKey(1).strMap1(ImmutableMap.of("A", ImmutableMap.of("A", "B"))).build();
        verify(result, expected, expected);
    }

    @Test
    void set_adding_nested_map_attributes_2() throws Throwable {
        storeItems(Model.builder().hashKey("A").rangeKey(1).strMap1(ImmutableMap.of("A", ImmutableMap.of("B", "C"))).build());
        final Method method = Dao.class.getMethod("set_adding_nested_map_attributes_2", String.class, String.class, String.class);

        final Model result = invoke(newUpdateMethod(method), "A", 1, ImmutableMap.of("A", ImmutableMap.of("B", "A")));

        System.out.println(result);
    }

    @Test
    void remove_attribute() throws Throwable {
        storeItems(Model.builder().hashKey("A").rangeKey(1).strMap1(ImmutableMap.of("A", ImmutableMap.of("B", "C"))).build());
        final Method method = Dao.class.getMethod("remove_attribute", String.class, String.class, String.class);

        final Model result = invoke(newUpdateMethod(method), "A", 1, "StrMap1");

        final Model expected = Model.builder().hashKey("A").rangeKey(1).build();
        verify(result, expected, expected);
    }

    @Test
    void remove_elements_from_a_list() throws Throwable {
        storeItems(Stream.of(Model.builder().hashKey("A").rangeKey(1).strList1(Arrays.asList("A", "B", "C")).build()));
        final Method method = Dao.class.getMethod("remove_elements_from_a_list", String.class, String.class);

        final Model result = invoke(newUpdateMethod(method), "A", 1);

        final Model expected = Model.builder().hashKey("A").rangeKey(1).strList1(Collections.singletonList("B")).build();
        verify(result, expected, expected);
    }

    @Test
    void add_a_number() throws Throwable {
        storeItems(Model.builder().hashKey("A").rangeKey(1).integer1(10).build());
        final Method method = Dao.class.getMethod("add_a_number", String.class, String.class, int.class);

        final Model result = invoke(newUpdateMethod(method), "A", 1, 20);

        final Model expected = Model.builder().hashKey("A").rangeKey(1).integer1(30).build();
        verify(result, expected, expected);
    }

    @Test
    void add_elements_to_a_set() throws Throwable {
        storeItems(Model.builder().hashKey("A").rangeKey(1).build());
        final Method method = Dao.class.getMethod("add_elements_to_a_set", String.class, String.class, Set.class);

        final Model result = invoke(newUpdateMethod(method), "A", 1, ImmutableSet.of("A", "B"));

        final Model expected = Model.builder().hashKey("A").rangeKey(1).strSet1(ImmutableSet.of("A", "B")).build();
        verify(result, expected, expected);
    }

    @Test
    void delete_elements_from_a_set() throws Throwable {
        storeItems(Model.builder().hashKey("A").rangeKey(1).strSet1(ImmutableSet.of("A", "B", "C")).build());
        final Method method = Dao.class.getMethod("delete_elements_to_a_set", String.class, String.class, Set.class);

        final Model result = invoke(newUpdateMethod(method), "A", 1, ImmutableSet.of("A", "B"));

        final Model expected = Model.builder().hashKey("A").rangeKey(1).strSet1(ImmutableSet.of("C")).build();
        verify(result, expected, expected);
    }

    private static Model invoke(final UpdateMethod updateMethod, final Object... args) throws Throwable {
        return (Model) updateMethod.invoke(args);
    }

    private UpdateMethod newUpdateMethod(final Method method) {
        return new UpdateMethod(Signature.resolve(method, Dao.class), getModelClass(), getDynamoDbMapper(), getDynamoDbClient(), null);
    }
}
