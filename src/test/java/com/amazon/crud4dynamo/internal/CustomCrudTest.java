package com.amazon.crud4dynamo.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.amazon.crud4dynamo.CrudForDynamo;
import com.amazon.crud4dynamo.annotation.Cached;
import com.amazon.crud4dynamo.annotation.Custom;
import com.amazon.crud4dynamo.annotation.Delete;
import com.amazon.crud4dynamo.annotation.MapperConfig;
import com.amazon.crud4dynamo.annotation.Parallel;
import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.annotation.Put;
import com.amazon.crud4dynamo.annotation.Query;
import com.amazon.crud4dynamo.annotation.Scan;
import com.amazon.crud4dynamo.annotation.Update;
import com.amazon.crud4dynamo.crudinterface.CompositeKeyCrud;
import com.amazon.crud4dynamo.exception.CrudForDynamoException;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.PageRequest;
import com.amazon.crud4dynamo.extension.PageResult;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazon.crud4dynamo.testbase.CompositeKeyTestBase;
import com.amazon.crud4dynamo.utility.PageResultCollector;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.SaveBehavior;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTypeConverted;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTypeConverter;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;

public class CustomCrudTest extends CompositeKeyTestBase<CustomCrudTest.Model, CustomCrudTest.CustomDao> {
    private static final String GSI = "GSI";
    private static final String LSI = "LSI";

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @DynamoDBTable(tableName = "TestTable")
    public static class Model {
        @DynamoDBHashKey(attributeName = "HashKey")
        private String hashKey;

        @DynamoDBRangeKey(attributeName = "RangeKey")
        private Integer rangeKey;

        @DynamoDBIndexRangeKey(attributeName = "LsiRangeKey", localSecondaryIndexName = LSI)
        private Integer lisRangeKey;

        @DynamoDBTypeConverted(converter = CompositeIndexHashKeyConverter.class)
        @DynamoDBIndexHashKey(attributeName = "IndexHashKey", globalSecondaryIndexName = GSI)
        private CompositeIndexHashKey indexKey;

        @DynamoDBAttribute(attributeName = "Str1")
        private String str1;
    }

    @Data
    @AllArgsConstructor
    public static class CompositeIndexHashKey {
        private String field1;
        private String field2;
    }

    public static class CompositeIndexHashKeyConverter implements DynamoDBTypeConverter<String, CompositeIndexHashKey> {
        private static final String DELIMITER = "_";

        @Override
        public String convert(final CompositeIndexHashKey object) {
            return String.join(DELIMITER, object.field1, object.field2);
        }

        @Override
        public CompositeIndexHashKey unconvert(final String object) {
            final String[] split = object.split(DELIMITER, 2);
            return new CompositeIndexHashKey(split[0], split[1]);
        }
    }

    public interface CustomDao extends CompositeKeyCrud<String, Integer, Model> {
        @Override
        Optional<Model> findBy(final String s, final Integer integer) throws CrudForDynamoException;

        default Optional<Model> findModelWithKey_A_and_One() {
            return findBy("A", 1);
        }

        @Query(keyCondition = "HashKey = :hashKey and RangeKey between :lower and :upper", scanIndexForward = false)
        PageResult<Model> groupByFilterAndReverse(
                @Param(":hashKey") final String hashKey,
                @Param(":lower") final int lower,
                @Param(":upper") final int upper,
                final PageRequest<Model> request);

        @Query(keyCondition = "HashKey = :hashKey and LsiRangeKey > :lower", scanIndexForward = false, index = LSI)
        Iterator<Model> sortByLsi(@Param(":hashKey") final String hashKey, @Param(":lower") final int lower);

        @Scan(filter = "Str1 <> :value")
        PageResult<Model> pageScan(@Param(":value") final String value, final PageRequest<Model> request);

        @Parallel(totalSegments = 2)
        @Scan(filter = "begins_with(Str1, :prefix)")
        Iterator<Model> parallelScanAndFilterByPrefix(@Param(":prefix") final String prefix);

        @Cached(expireAfterAccess = 2, expireAfterAccessTimeUnit = TimeUnit.SECONDS)
        default List<Model> convertFindAllToList_andCache() {
            return Lists.newArrayList(findAll());
        }

        @Custom(factoryClass = CustomMethodFactory.class)
        Model customMethod();

        @Update(
                keyExpression = "HashKey = :hashKey, RangeKey = :rangeKey",
                updateExpression = "SET Str1 = :value",
                returnValue = ReturnValue.ALL_NEW)
        Model update(@Param(":hashKey") final String hashKey, @Param(":rangeKey") final int rangeKey, @Param(":value") final String value);

        @Put(conditionExpression = "attribute_exists(#attributeName)", returnValue = ReturnValue.ALL_OLD)
        Model put(@Param(":item") Model model, @Param("#attributeName") final String attributeName);

        @MapperConfig(saveBehavior = SaveBehavior.UPDATE_SKIP_NULL_ATTRIBUTES)
        void save(final Model model) throws CrudForDynamoException;

        @Delete(
                keyExpression = "HashKey = :hashKey, RangeKey = :rangeKey",
                conditionExpression = "attribute_exists(#attributeName)",
                returnValue = ReturnValue.ALL_OLD)
        Model delete(
                @Param(":hashKey") final String hashKey,
                @Param(":rangeKey") final Integer rangeKey,
                @Param("#attributeName") final String attributeName);
    }

    public static class CustomMethodFactory implements AbstractMethodFactory {

        @Override
        public AbstractMethod create(final Context context) {
            return new AbstractMethod() {
                @Override
                public Signature getSignature() {
                    return Signature.resolve(context.method(), CustomDao.class);
                }

                @Override
                public Object invoke(final Object... args) throws Throwable {
                    return Model.builder().hashKey("CustomMethod").rangeKey(1).build();
                }

                @Override
                public AbstractMethod bind(final Object target) {
                    return this;
                }
            };
        }
    }

    @Override
    protected CustomDao newDao() {
        return new CrudForDynamo(getDynamoDbClient()).create(CustomDao.class);
    }

    @Override
    protected List<Model> getTestData() {
        return Arrays.asList(
                Model.builder().hashKey("A").rangeKey(1).indexKey(new CompositeIndexHashKey("I", "1")).str1("A").build(),
                Model.builder().hashKey("A").rangeKey(2).indexKey(new CompositeIndexHashKey("I", "1")).str1("B").build(),
                Model.builder().hashKey("A").rangeKey(3).indexKey(new CompositeIndexHashKey("I", "2")).str1("C").build(),
                Model.builder().hashKey("A").rangeKey(4).indexKey(new CompositeIndexHashKey("I", "3")).str1("D").build(),
                Model.builder().hashKey("B").rangeKey(1).indexKey(new CompositeIndexHashKey("I", "1")).str1("E").build());
    }

    @Override
    protected Class<Model> getModelClass() {
        return Model.class;
    }

    @Test
    void canHandleDefaultMethod() {
        saveAll();

        assertThat(getDao().findModelWithKey_A_and_One()).contains(getTestData().get(0));
    }

    @Test
    void groupByFilterAndReverse() {
        saveAll();

        final List<Model> models =
                PageResultCollector.newCollector(
                                PageRequest.<Model>builder().limit(1).build(), req -> getDao().groupByFilterAndReverse("A", 2, 3, req))
                        .get();

        assertThat(models).containsExactly(getTestData().get(2), getTestData().get(1));
    }

    @Test
    void sortByLsi() {
        final String hashKey = "AA";
        final List<Model> models =
                storeItems(
                        Stream.of(
                                Model.builder().hashKey(hashKey).rangeKey(5).lisRangeKey(1).build(),
                                Model.builder().hashKey(hashKey).rangeKey(4).lisRangeKey(2).build(),
                                Model.builder().hashKey(hashKey).rangeKey(3).lisRangeKey(3).build()));
        Collections.reverse(models);

        final List<Model> returned = Lists.newArrayList(getDao().sortByLsi(hashKey, 0));

        assertThat(returned).containsExactlyElementsOf(models);
    }

    @Test
    void parallelScanAndFilterByPrefix() {
        final String hashKey = "AA";
        final List<Model> models =
                storeItems(
                        Stream.of(
                                Model.builder().hashKey(hashKey).rangeKey(5).str1("Metaprogramming").build(),
                                Model.builder().hashKey(hashKey).rangeKey(4).str1("Meta Data").build(),
                                Model.builder().hashKey(hashKey).rangeKey(3).str1("eMta").build()));

        final List<Model> returned = Lists.newArrayList(getDao().parallelScanAndFilterByPrefix("Meta"));

        assertThat(returned).containsOnlyElementsOf(models.subList(0, 2));
    }

    @Test
    void convertFindAllToList_andCache() throws Exception {
        final Model model = Model.builder().hashKey("test").rangeKey(1).str1("test").build();
        final Model modelChanged = Model.builder().hashKey("test").rangeKey(1).str1("tested").build();

        getDao().save(model);
        getDao().convertFindAllToList_andCache();
        getDao().save(modelChanged);

        assertThat(getDao().convertFindAllToList_andCache()).contains(model);
        assertThat(getDao().convertFindAllToList_andCache()).doesNotContain(modelChanged);

        TimeUnit.SECONDS.sleep(2);

        assertThat(getDao().convertFindAllToList_andCache()).doesNotContain(model);
        assertThat(getDao().convertFindAllToList_andCache()).contains(modelChanged);
    }

    @Test
    void customMethod() {
        final Model model = getDao().customMethod();

        assertThat(model).isEqualTo(Model.builder().hashKey("CustomMethod").rangeKey(1).build());
    }

    @Test
    void updateMethod() {
        storeItems(Model.builder().hashKey("A").rangeKey(10).build());

        final Model updated = getDao().update("A", 10, "Hello World");

        assertThat(updated).isEqualTo(Model.builder().hashKey("A").rangeKey(10).str1("Hello World").build());
    }

    @Test
    void pageScan() {
        saveAll();

        final List<Model> results =
                PageResultCollector.newCollector(PageRequest.<Model>builder().limit(1).build(), req -> getDao().pageScan("A", req)).get();

        assertThat(results).containsOnlyElementsOf(getTestData().subList(1, getTestData().size()));
    }

    @Test
    void saveWith_UPDATE_SKIP_NULL_ATTRIBUTES_SaveBehavior() {
        final String hashKey = "hashKey";
        final int rangeKey = 10;
        final String str = "oops";
        final Model model = Model.builder().hashKey(hashKey).rangeKey(rangeKey).str1(str).build();

        getDao().save(model);

        assertThat(getDao().findBy(hashKey, rangeKey)).contains(model);

        getDao().save(Model.builder().hashKey(hashKey).rangeKey(rangeKey).build());
        /**
         * UPDATE_SKIP_NULL_ATTRIBUTES is similar to UPDATE, except that it ignores any null value attribute(s) and will NOT remove them
         * from that item in DynamoDB. It also guarantees to send only one single updateItem request, no matter the object is key-only or
         * not.
         */
        assertThat(getDao().findBy(hashKey, rangeKey)).contains(model);
    }

    @Test
    void put_failedConditionalCheck_throwException() throws Exception {
        final Model model = Model.builder().hashKey("put_failed_conditional_check").rangeKey(3).str1("test").build();

        assertThatThrownBy(() -> getDao().put(model, "Str1")).isInstanceOf(ConditionalCheckFailedException.class);
    }

    @Test
    void put() throws Exception {
        final Model oldModel = Model.builder().hashKey("put").rangeKey(3).str1("test").build();
        final Model newModel = Model.builder().hashKey("put").rangeKey(3).build();
        getDao().save(oldModel);

        final Model result = getDao().put(newModel, "Str1");

        assertThat(result).isEqualTo(oldModel);
    }

    @Test
    void delete_failedConditionalCheck_throwException() throws Exception {
        final String dummyHashKey = "delete_failed_conditional_check";
        final Integer dummyRangeKey = 3;
        final Model model = Model.builder().hashKey(dummyHashKey).rangeKey(dummyRangeKey).build();
        getDao().save(model);

        assertThatThrownBy(() -> getDao().delete(dummyHashKey, dummyRangeKey, "Str1")).isInstanceOf(ConditionalCheckFailedException.class);
    }

    @Test
    void conditionalDeleteSucceeded() {
        final String dummyHashKey = "hashKey";
        final Integer dummyRangeKey = 3;
        final Model model = Model.builder().hashKey(dummyHashKey).rangeKey(dummyRangeKey).str1("str").build();
        getDao().save(model);

        final Model deleted = getDao().delete(dummyHashKey, dummyRangeKey, "Str1");

        assertThat(deleted).isEqualTo(model);
    }
}
