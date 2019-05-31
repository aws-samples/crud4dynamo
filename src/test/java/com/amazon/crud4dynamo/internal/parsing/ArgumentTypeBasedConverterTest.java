package com.amazon.crud4dynamo.internal.parsing;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.amazon.crud4dynamo.exception.CrudForDynamoException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.util.DateUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ArgumentTypeBasedConverterTest {
  private static final String DUMMY_PATH = "DUMMY_PATH";

  @Test
  void stringType() {
    final String value = "ABC";

    assertThat(convert(value)).isEqualTo(new AttributeValue().withS(value));
  }

  @Test
  void number() {
    final int value = 1;

    assertThat(convert(value)).isEqualTo(new AttributeValue().withN(Integer.toString(value)));
  }

  @Test
  void booleanType() {
    final boolean value = true;

    assertThat(convert(value)).isEqualTo(new AttributeValue().withBOOL(value));
  }

  @Test
  void byteArray() {
    final byte[] value = {0x1, 0xf};

    assertThat(convert(value)).isEqualTo(new AttributeValue().withB(ByteBuffer.wrap(value)));
  }

  @Test
  void byteBuffer() {
    final ByteBuffer value = ByteBuffer.wrap(new byte[] {0x1});

    assertThat(convert(value)).isEqualTo(new AttributeValue().withB(value));
  }

  @Test
  void nested_map1() {
    final Map<String, Map<String, Integer>> amap = ImmutableMap.of("A", ImmutableMap.of("B", 1));

    final AttributeValue attributeValue = convert(amap);

    assertThat(attributeValue)
        .isEqualTo(
            new AttributeValue()
                .addMEntry(
                    "A", new AttributeValue().addMEntry("B", new AttributeValue().withN("1"))));
  }

  @Test
  void nested_map2() {
    final Map<String, Map<Integer, Integer>> amap = ImmutableMap.of("A", ImmutableMap.of(3, 1));

    assertThatThrownBy(() -> convert(amap)).isInstanceOf(CrudForDynamoException.class);
  }

  @Test
  void list1() {
    final List<Object> alist = ImmutableList.of("A", ImmutableMap.of("A", "B"));

    assertThat(convert(alist))
        .isEqualTo(
            new AttributeValue()
                .withL(
                    new AttributeValue("A"),
                    new AttributeValue().addMEntry("A", new AttributeValue("B"))));
  }

  @Test
  void string_set() {
    final Set<String> aset = ImmutableSet.of("A", "B", "C");

    assertThat(convert(aset)).isEqualTo(new AttributeValue().withSS("A", "B", "C"));
  }

  @Test
  void number_set() {
    final Set<? extends Number> aset = ImmutableSet.of(1, 2.0);

    assertThat(convert(aset)).isEqualTo(new AttributeValue().withNS("1", "2.0"));
  }

  @Test
  void binary_set() {
    final Set<Object> aset =
        ImmutableSet.of(new byte[] {0x1}, new byte[] {0x2}, ByteBuffer.wrap(new byte[] {0x3}));

    assertThat(convert(aset))
        .isEqualTo(
            new AttributeValue()
                .withBS(
                    ByteBuffer.wrap(new byte[] {0x1}),
                    ByteBuffer.wrap(new byte[] {0x2}),
                    ByteBuffer.wrap(new byte[] {0x3})));
  }

  @Test
  void heterogeneous_set() {
    final Set<Object> aset = ImmutableSet.of(new byte[] {0x1}, 3);

    assertThatThrownBy(() -> convert(aset)).isInstanceOf(CrudForDynamoException.class);
  }

  @Test
  void date() {
    final LocalDate now = LocalDate.of(1984, 4, 2);
    final Date date = Date.from(getInstant(now));
    final String s = DateUtils.formatISO8601Date(date);

    assertThat(convert(date)).isEqualTo(new AttributeValue().withS(s));
  }

  private Instant getInstant(final LocalDate now) {
    return now.atStartOfDay().toInstant(ZoneOffset.UTC);
  }

  @Test
  void calendar() {
    final LocalDate now = LocalDate.of(1984, 4, 2);
    final Date date = Date.from(getInstant(now));
    final Calendar calendar = new Calendar.Builder().setInstant(date).build();
    final String s = DateUtils.formatISO8601Date(date);

    assertThat(convert(calendar)).isEqualTo(new AttributeValue().withS(s));
  }

  @Test
  void unsupportedType_throwException() {
    assertThatThrownBy(() -> convert(new Object())).isInstanceOf(CrudForDynamoException.class);
  }

  private AttributeValue convert(final Object object) {
    return new ArgumentTypeBasedConverter(DUMMY_PATH).convert(object);
  }
}
