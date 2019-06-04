/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.crud4dynamo.compatibility.mapperannotation;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazon.crud4dynamo.CrudForDynamo;
import com.amazon.crud4dynamo.annotation.Param;
import com.amazon.crud4dynamo.annotation.Query;
import com.amazon.crud4dynamo.annotation.Scan;
import com.amazon.crud4dynamo.compatibility.Book;
import com.amazon.crud4dynamo.compatibility.Book.Attributes;
import com.amazon.crud4dynamo.compatibility.Book.CustomDate;
import com.amazon.crud4dynamo.compatibility.Book.GSI;
import com.amazon.crud4dynamo.compatibility.Book.Picture;
import com.amazon.crud4dynamo.crudinterface.CompositeKeyCrud;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.google.common.collect.Lists;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class QueryAndScanMapperAnnotationTest extends SingleTableDynamoDbTestBase<Book> {

  private BookDao bookDao;

  @Override
  protected Class<Book> getModelClass() {
    return Book.class;
  }

  @Override
  @BeforeEach
  public void setUp() throws Exception {
    super.setUp();

    bookDao = new CrudForDynamo(getDynamoDbClient()).create(BookDao.class);
  }

  @Test
  void query_annotation_DynamoDBTyped() {
    final String dummyAuthor = "dummy author";
    final int dummyInteger1 = 123;
    final int dummyInteger2 = 234;
    final Book item1 =
        Book.builder().author(dummyAuthor).integerStoredAsString(dummyInteger1).build();
    bookDao.save(item1);
    final Book item2 =
        Book.builder().author(dummyAuthor).integerStoredAsString(dummyInteger2).build();
    bookDao.save(item2);

    final List<Book> books =
        Lists.newArrayList(bookDao.queryFilterByIntegerStoredAsString(dummyAuthor, dummyInteger1));

    assertThat(books).hasSize(1);
    assertThat(books.get(0)).isEqualTo(item2);
  }

  @Test
  void scan_annotation_DynamoDBTyped() {
    final String dummyAuthor = "dummy author";
    final int dummyInteger1 = 123;
    final int dummyInteger2 = 234;
    final Book item1 =
        Book.builder().author(dummyAuthor).integerStoredAsString(dummyInteger1).build();
    bookDao.save(item1);
    final Book item2 =
        Book.builder().author(dummyAuthor).integerStoredAsString(dummyInteger2).build();
    bookDao.save(item2);

    final List<Book> books =
        Lists.newArrayList(bookDao.filterByIntegerStoredAsString(dummyInteger1));

    assertThat(books).hasSize(1);
    assertThat(books.get(0)).isEqualTo(item2);
  }

  @Test
  void query_annotation_DynamoDBTypeConverted() {
    final String dummyAuthor = "dummy author";
    final CustomDate customDate = CustomDate.builder().year(1984).month(4).day(1).build();
    final Book bookItem = Book.builder().author(dummyAuthor).customDate(customDate).build();
    bookDao.save(bookItem);

    final List<Book> books =
        Lists.newArrayList(bookDao.queryFilterByCustomDate(dummyAuthor, customDate));
    assertThat(books).hasSize(1);
    assertThat(books.get(0)).isEqualTo(bookItem);
  }

  @Test
  void scan_annotation_DynamoDBTypeConverted() {
    final String dummyAuthor = "dummy author";
    final CustomDate customDate = CustomDate.builder().year(1984).month(4).day(1).build();
    final Book bookItem = Book.builder().author(dummyAuthor).customDate(customDate).build();
    bookDao.save(bookItem);

    final List<Book> books = Lists.newArrayList(bookDao.filterByCustomDate(customDate));
    assertThat(books).hasSize(1);
    assertThat(books.get(0)).isEqualTo(bookItem);
  }

  @Test
  void query_annotation_DynamoDBDocument() {
    final String dummyAuthor = "dummy author";
    final Picture cover = Picture.builder().height(100).width(100).sourcePath("a/path").build();
    final Book bookItem = Book.builder().author(dummyAuthor).cover(cover).build();
    bookDao.save(bookItem);
    bookDao.save(Book.builder().author("another dummy author").build());

    final List<Book> books = Lists.newArrayList(bookDao.queryFilterByCover(dummyAuthor, cover));

    assertThat(books).hasSize(1);
    assertThat(books.get(0)).isEqualTo(bookItem);
  }

  @Test
  void scan_annotation_DynamoDBDocument() {
    final String dummyAuthor = "dummy author";
    final Picture cover = Picture.builder().height(100).width(100).sourcePath("a/path").build();
    final Book bookItem = Book.builder().author(dummyAuthor).cover(cover).build();
    bookDao.save(bookItem);
    bookDao.save(Book.builder().author("another dummy author").build());

    final List<Book> books = Lists.newArrayList(bookDao.filterByCover(cover));

    assertThat(books).hasSize(1);
    assertThat(books.get(0)).isEqualTo(bookItem);
  }

  @Test
  void query_gsi() {
    final String dummyAuthor1 = "dummy author1";
    final String dummyAuthor2 = "dummy author2";
    final String title = "dummy title";
    final CustomDate customDate = CustomDate.builder().year(1984).month(4).day(1).build();
    final Book bookItem1 =
        Book.builder().author(dummyAuthor1).title(title).customDate(customDate).build();
    final Book bookItem2 =
        Book.builder().author(dummyAuthor2).title(title).customDate(customDate).build();
    bookDao.save(bookItem1);
    bookDao.save(bookItem2);

    final List<Book> books =
        Lists.newArrayList(bookDao.queryGsiFilterByAuthor(customDate, title, dummyAuthor1));

    assertThat(books).hasSize(1);
    assertThat(books.get(0)).isEqualTo(bookItem2);
  }

  @Test
  void scan_gsi() {
    final String dummyAuthor1 = "dummy author1";
    final String dummyAuthor2 = "dummy author2";
    final String title = "dummy title";
    final CustomDate customDate = CustomDate.builder().year(1984).month(4).day(1).build();
    final Book bookItem1 =
        Book.builder().author(dummyAuthor1).title(title).customDate(customDate).build();
    final Book bookItem2 =
        Book.builder().author(dummyAuthor2).title(title).customDate(customDate).build();
    bookDao.save(bookItem1);
    bookDao.save(bookItem2);

    final List<Book> books = Lists.newArrayList(bookDao.scanGsiFilterByAuthor(dummyAuthor1));

    assertThat(books).hasSize(1);
    assertThat(books.get(0)).isEqualTo(bookItem2);
  }

  public interface BookDao extends CompositeKeyCrud<String, Integer, Book> {

    @Query(
        keyCondition = Attributes.HASH_KEY + " = :hashKey",
        filter = Attributes.INTEGER_STORED_AS_STRING + " <> " + " :value")
    Iterator<Book> queryFilterByIntegerStoredAsString(
        @Param(":hashKey") final String hashKey, @Param(":value") final int value);

    @Scan(filter = Attributes.INTEGER_STORED_AS_STRING + " <> " + " :value")
    Iterator<Book> filterByIntegerStoredAsString(@Param(":value") final int value);

    @Query(
        keyCondition = Attributes.HASH_KEY + " = :hashKey",
        filter = Attributes.CUSTOM_DATE + " = " + " :value")
    Iterator<Book> queryFilterByCustomDate(
        @Param(":hashKey") final String hashKey, @Param(":value") final CustomDate customDate);

    @Scan(filter = Attributes.CUSTOM_DATE + " = " + " :value")
    Iterator<Book> filterByCustomDate(@Param(":value") final CustomDate customDate);

    @Query(
        keyCondition = Attributes.HASH_KEY + " = :hashKey",
        filter = Attributes.COVER + " = " + " :value")
    Iterator<Book> queryFilterByCover(
        @Param(":hashKey") final String hashKey, @Param(":value") final Picture cover);

    @Scan(filter = Attributes.COVER + " = " + " :value")
    Iterator<Book> filterByCover(@Param(":value") final Picture cover);

    @Query(
        keyCondition = Book.GSI.HASH_KEY + " = :hashKey AND " + GSI.RANGE_KEY + " = :rangeKey",
        index = GSI.NAME,
        filter = Attributes.HASH_KEY + " <> " + " :author")
    Iterator<Book> queryGsiFilterByAuthor(
        @Param(":hashKey") final CustomDate hashKey,
        @Param(":rangeKey") final String rangeKey,
        @Param(":author") final String author);

    @Scan(index = GSI.NAME, filter = Attributes.HASH_KEY + " <> " + " :author")
    Iterator<Book> scanGsiFilterByAuthor(@Param(":author") final String author);
  }
}
