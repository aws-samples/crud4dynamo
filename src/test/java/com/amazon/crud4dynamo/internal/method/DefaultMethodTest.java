package com.amazon.crud4dynamo.internal.method;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.amazon.crud4dynamo.exception.CrudForDynamoException;
import com.amazon.crud4dynamo.extension.Signature;
import com.google.common.reflect.AbstractInvocationHandler;
import com.google.common.reflect.Reflection;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

class DefaultMethodTest {

  private static final String SUFFIX = "suffix";
  private static final String PREFIX = "prefix";
  private static final String CONTENT = "content";

  public interface TestInterface {

    default String suffix() {
      return SUFFIX;
    }

    String prefix();

    default String content() {
      return prefix() + suffix();
    }
  }

  private static final TestInterface TEST_INTERFACE_IMPL = () -> PREFIX;

  @Test
  void getSignature() throws Throwable {
    final DefaultMethod method = getDefaultMethod(SUFFIX);

    final Signature signature = method.getSignature();

    assertThat(signature)
        .isEqualTo(Signature.resolve(TestInterface.class.getMethod(SUFFIX), TestInterface.class));
  }

  @Test
  public void defaultMethod() throws Throwable {
    final DefaultMethod method = getDefaultMethod(SUFFIX);

    final Object result = method.bind(TEST_INTERFACE_IMPL).invoke();

    assertThat(result).isEqualTo(SUFFIX);
  }

  @Test
  public void methodIsNotBind_throwException() {
    assertThatThrownBy(() -> getDefaultMethod(SUFFIX).invoke())
        .isInstanceOf(CrudForDynamoException.class);
  }

  @Test
  public void usingProxy_delegateProperly() throws Throwable {
    final TestInterface proxy = createProxy();

    final String content = proxy.content();

    assertThat(content).isEqualTo(PREFIX + SUFFIX);
  }

  private TestInterface createProxy() throws Throwable {
    final DefaultMethod contentMethod = getDefaultMethod(CONTENT);
    final DefaultMethod suffixMethod = getDefaultMethod(SUFFIX);
    return Reflection.newProxy(
        TestInterface.class,
        new AbstractInvocationHandler() {
          @Override
          protected Object handleInvocation(
              final Object proxy, final Method method, final Object[] args) throws Throwable {
            switch (method.getName()) {
              case PREFIX:
                return TEST_INTERFACE_IMPL.prefix();
              case SUFFIX:
                return suffixMethod.bind(proxy).invoke(args);
              case CONTENT:
                return contentMethod.bind(proxy).invoke(args);
              default:
                throw new RuntimeException("unknown method");
            }
          }
        });
  }

  private static DefaultMethod getDefaultMethod(final String name) throws Throwable {
    final Method method = TestInterface.class.getMethod(name);
    return new DefaultMethod(method, Signature.resolve(method, TestInterface.class));
  }
}
