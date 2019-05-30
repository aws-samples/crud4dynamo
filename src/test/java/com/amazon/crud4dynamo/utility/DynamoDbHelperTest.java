package com.amazon.crud4dynamo.utility;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazon.crud4dynamo.testdata.DummyTable;
import org.junit.jupiter.api.Test;

class DynamoDbHelperTest {
    @Test
    void getTableName() {
        final String tableName = DynamoDbHelper.getTableName(DummyTable.class);

        assertThat(tableName).isEqualTo(DummyTable.NAME);
    }
}
