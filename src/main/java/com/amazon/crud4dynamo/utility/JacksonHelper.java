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

package com.amazon.crud4dynamo.utility;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class JacksonHelper {
  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  public static <T> T deserialize(final String json, final Class<T> type) {
    try {
      return OBJECT_MAPPER.readValue(json, type);
    } catch (final IOException e) {
      throw ExceptionHelper.throwAsUnchecked(e);
    }
  }

  public static String serialize(final Object object) {
    try {
      return OBJECT_MAPPER.writeValueAsString(object);
    } catch (final IOException e) {
      throw ExceptionHelper.throwAsUnchecked(e);
    }
  }

  public static String toPrettyJsonString(final Object object) {
    try {
      return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(object);
    } catch (final IOException e) {
      throw ExceptionHelper.throwAsUnchecked(e);
    }
  }
}
