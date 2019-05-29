package com.amazon.crud4dynamo.utility;

import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Constructor;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MethodHandlesHelper {

    public static Lookup getLookup(final Class<?> declaringClass) {
        try {
            final Constructor<Lookup> constructor = Lookup.class.getDeclaredConstructor(Class.class, int.class);
            constructor.setAccessible(true);
            final Lookup lookup = constructor.newInstance(declaringClass, Lookup.PRIVATE);
            return lookup;
        } catch (final Exception e) {
            throw ExceptionHelper.throwAsUnchecked(e);
        }
    }
}
