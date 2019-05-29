package com.amazon.crud4dynamo.utility;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ExceptionHelper {
    public static RuntimeException throwAsUnchecked(final Exception exception) {
        throw ExceptionHelper.<RuntimeException>throwAsUncheckedInner(exception);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Exception> T throwAsUncheckedInner(final Exception exception) throws T {
        throw (T) exception;
    }
}
