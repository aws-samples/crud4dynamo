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
import com.amazon.crud4dynamo.annotation.Update;
import com.amazon.crud4dynamo.compatibility.Book;
import com.amazon.crud4dynamo.compatibility.Book.Attributes;
import com.amazon.crud4dynamo.compatibility.Book.CustomDate;
import com.amazon.crud4dynamo.compatibility.Book.Picture;
import com.amazon.crud4dynamo.crudinterface.CompositeKeyCrud;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.google.common.collect.Lists;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class UpdateMethodsMapperAnnotationTest extends SingleTableDynamoDbTestBase<Book> {

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
  void annotation_DynamoDBTypeConverted() {
    final String dummyAuthor = "dummy author";
    final Book book = Book.builder().author(dummyAuthor).build();
    bookDao.save(book);
    List<Book> books = Lists.newArrayList(bookDao.groupBy(dummyAuthor));
    assertThat(books).hasSize(1);
    assertThat(books.get(0)).isEqualTo(book);

    final CustomDate customDate = CustomDate.builder().year(1984).month(4).day(1).build();
    bookDao.updateCustomDate(book.getAuthor(), book.getId(), customDate);

    books = Lists.newArrayList(bookDao.groupBy(dummyAuthor));
    assertThat(books).hasSize(1);
    assertThat(books.get(0).getCustomDate()).isEqualTo(customDate);
  }

  @Test
  void annotation_DynamoDBDocument() {
    final String dummyAuthor = "dummy author";
    final Book book = Book.builder().author(dummyAuthor).build();
    bookDao.save(book);
    List<Book> books = Lists.newArrayList(bookDao.groupBy(dummyAuthor));
    assertThat(books).hasSize(1);
    assertThat(books.get(0)).isEqualTo(book);

    final Picture picture = Picture.builder().width(100).height(200).sourcePath("a/path").build();
    bookDao.updateCover(book.getAuthor(), book.getId(), picture);

    books = Lists.newArrayList(bookDao.groupBy(dummyAuthor));
    assertThat(books).hasSize(1);
    assertThat(books.get(0).getCover()).isEqualTo(picture);
  }

  @Test
  void annotation_DynamoDBTyped() {
    final String dummyAuthor = "dummy author";
    final Book book = Book.builder().author(dummyAuthor).build();
    bookDao.save(book);

    List<Book> books = Lists.newArrayList(bookDao.groupBy(dummyAuthor));
    assertThat(books).hasSize(1);

    final int dummyInteger = 123;
    bookDao.updateIntegerStoredAsString(dummyAuthor, book.getId(), dummyInteger);

    books = Lists.newArrayList(bookDao.groupBy(dummyAuthor));
    assertThat(books).hasSize(1);
    assertThat(books.get(0).getIntegerStoredAsString()).isEqualTo(dummyInteger);
  }

  public interface BookDao extends CompositeKeyCrud<String, Integer, Book> {

    @Update(
        keyExpression =
            Attributes.HASH_KEY + " = :hashKey, " + Attributes.RANGE_KEY + " = :rangeKey",
        updateExpression = "SET " + Attributes.CUSTOM_DATE + " = :value")
    Book updateCustomDate(
        @Param(":hashKey") final String hashKey,
        @Param(":rangeKey") final String rangeKey,
        @Param(":value") final CustomDate customDate);

    @Update(
        keyExpression =
            Attributes.HASH_KEY + " = :hashKey, " + Attributes.RANGE_KEY + " = :rangeKey",
        updateExpression = "SET " + Attributes.COVER + " = :value")
    Book updateCover(
        @Param(":hashKey") final String hashKey,
        @Param(":rangeKey") final String rangeKey,
        @Param(":value") final Picture cover);

    @Update(
        keyExpression =
            Attributes.HASH_KEY + " = :hashKey, " + Attributes.RANGE_KEY + " = :rangeKey",
        updateExpression = "SET " + Attributes.INTEGER_STORED_AS_STRING + " = :value")
    Book updateIntegerStoredAsString(
        @Param(":hashKey") final String hashKey,
        @Param(":rangeKey") final String rangeKey,
        @Param(":value") final int value);
  }
}
