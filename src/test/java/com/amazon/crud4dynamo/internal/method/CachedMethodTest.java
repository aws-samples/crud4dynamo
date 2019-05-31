package com.amazon.crud4dynamo.internal.method;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazon.crud4dynamo.annotation.Cached;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class CachedMethodTest {
  private static final int EXPIRE_AFTER_WRITE = 2;

  public interface TestInterface {
    @Cached
    int aMethod();

    @Cached(expireAfterWrite = EXPIRE_AFTER_WRITE, expireAfterWriteTimeUnit = TimeUnit.SECONDS)
    int methodWithoutArgument();

    @Cached(expireAfterWrite = EXPIRE_AFTER_WRITE, expireAfterWriteTimeUnit = TimeUnit.SECONDS)
    int methodWithArgument(int i);
  }

  @Nested
  class BasicMethodVerification {

    private CachedMethod cachedMethod;
    private Method method;
    private AbstractMethod delegate;

    @BeforeEach
    void setUp() throws Throwable {
      delegate = mock(AbstractMethod.class);
      method = TestInterface.class.getMethod("aMethod");
      cachedMethod = getCachedMethod(delegate, method);
    }

    @Test
    void getSignature() {
      final Signature signature = cachedMethod.getSignature();

      assertThat(signature).isEqualTo(Signature.resolve(method, TestInterface.class));
    }

    @Test
    void invokeDelegateBind() throws Throwable {
      final Object dummy = new Object();

      cachedMethod.bind(dummy);

      verify(delegate).bind(dummy);
    }
  }

  private CachedMethod getCachedMethod(final AbstractMethod delegate, final Method method) {
    final Signature signature = Signature.resolve(method, TestInterface.class);
    return new CachedMethod(signature, delegate);
  }

  @Test
  void cachedMethodWithoutArgument() throws Throwable {
    final AbstractMethod delegate = mock(AbstractMethod.class);
    when(delegate.invoke()).thenReturn(1).thenReturn(2);
    final Method method = TestInterface.class.getMethod("methodWithoutArgument");
    final CachedMethod cachedMethod = getCachedMethod(delegate, method);

    assertThat(cachedMethod.invoke()).isEqualTo(1);
    assertThat(cachedMethod.invoke()).isEqualTo(1);
    verify(delegate, times(1)).invoke();

    waitExpire();

    assertThat(cachedMethod.invoke()).isEqualTo(2);
    assertThat(cachedMethod.invoke()).isEqualTo(2);

    verify(delegate, times(2)).invoke();
  }

  @Test
  void cachedMethodWithArgument() throws Throwable {
    final AbstractMethod delegate = mock(AbstractMethod.class);
    when(delegate.invoke(1)).thenReturn(1).thenReturn(3);
    when(delegate.invoke(2)).thenReturn(2).thenReturn(4);
    final Method method = TestInterface.class.getMethod("methodWithArgument", int.class);
    final CachedMethod cachedMethod = getCachedMethod(delegate, method);

    assertThat(cachedMethod.invoke(1)).isEqualTo(1);
    assertThat(cachedMethod.invoke(1)).isEqualTo(1);
    assertThat(cachedMethod.invoke(2)).isEqualTo(2);
    assertThat(cachedMethod.invoke(2)).isEqualTo(2);
    verify(delegate, times(1)).invoke(1);
    verify(delegate, times(1)).invoke(2);

    waitExpire();

    assertThat(cachedMethod.invoke(1)).isEqualTo(3);
    assertThat(cachedMethod.invoke(1)).isEqualTo(3);
    assertThat(cachedMethod.invoke(2)).isEqualTo(4);
    assertThat(cachedMethod.invoke(2)).isEqualTo(4);

    verify(delegate, times(2)).invoke(1);
    verify(delegate, times(2)).invoke(2);
  }

  private void waitExpire() throws InterruptedException {
    TimeUnit.SECONDS.sleep(EXPIRE_AFTER_WRITE);
  }
}
