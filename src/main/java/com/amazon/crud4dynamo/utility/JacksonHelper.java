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
