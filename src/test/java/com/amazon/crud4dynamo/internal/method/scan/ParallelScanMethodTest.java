package com.amazon.crud4dynamo.internal.method.scan;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazon.crud4dynamo.annotation.Parallel;
import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.annotation.Scan;
import com.amazon.crud4dynamo.crudinterface.CompositeKeyCrud;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.internal.method.scan.ParallelScanMethodTest.Model;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;

class ParallelScanMethodTest extends SingleTableDynamoDbTestBase<Model> {
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
    protected Class getModelClass() {
        return Model.class;
    }

    public interface Dao extends CompositeKeyCrud<String, Integer, Model> {
        @Parallel(totalSegments = 10)
        @Scan(filter = "#rangeKey between :lower and :upper")
        Iterator<Model> scan(@Param("#rangeKey") String rangeKeyName, @Param(":lower") int lower, @Param(":upper") int upper);
    }

    @Test
    void parallelScan() throws Throwable {
        final List<Model> models = storeItems(prepareData(10));
        final ParallelScanMethod method = getMethod();

        final List<Model> scanResult = Lists.newArrayList((Iterator<Model>) method.invoke("RangeKey", 3, 7));

        assertThat(scanResult).containsAll(models.subList(3, 8));
    }

    private ParallelScanMethod getMethod() throws NoSuchMethodException {
        final Signature signature = Signature.resolve(Dao.class.getMethod("scan", String.class, int.class, int.class), Dao.class);
        return new ParallelScanMethod(signature, getModelClass(), getDynamoDbMapper(), null);
    }

    private static Stream<Model> prepareData(final int numItems) {
        return IntStream.range(0, numItems).mapToObj(i -> Model.builder().hashKey(Integer.toHexString(i)).rangeKey(i).build());
    }
}
