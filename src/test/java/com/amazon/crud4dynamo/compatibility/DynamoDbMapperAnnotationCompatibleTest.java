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

package com.amazon.crud4dynamo.compatibility;

import com.amazon.crud4dynamo.CrudForDynamo;
import com.amazon.crud4dynamo.compatibility.Book.Picture;
import com.amazon.crud4dynamo.crudinterface.SimpleKeyCrud;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;


public class DynamoDbMapperAnnotationCompatibleTest extends
    SingleTableDynamoDbTestBase<Book> {

  @Override
  protected Class<Book> getModelClass() {
    return Book.class;
  }

  @Test
  void name() {
    final SimpleKeyCrud<String, Book> bookDao =
        new CrudForDynamo(getDynamoDbClient()).createSimple(Book.class);

    final Book build = Book.builder()
        .title("The Little Typer")
        .author("Daniel Friedman")
        .cover(new Picture(300, 400, "a/path/to"))
        .build();

    bookDao.save(build);

    Lists.newArrayList(bookDao.findAll()).forEach(System.out::println);
  }
}
