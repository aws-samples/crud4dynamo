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

import com.amazon.crud4dynamo.CrudForDynamo;
import com.amazon.crud4dynamo.compatibility.Book;
import com.amazon.crud4dynamo.crudinterface.CompositeKeyCrud;
import com.amazon.crud4dynamo.testbase.SingleTableDynamoDbTestBase;
import org.junit.jupiter.api.BeforeEach;

public class PutMethodsMapperAnnotationTest extends SingleTableDynamoDbTestBase<Book> {
  public interface BookDao extends CompositeKeyCrud<String, Integer, Book> {

  }

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
}
