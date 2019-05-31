package com.amazon.crud4dynamo.internal.method;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazon.crud4dynamo.extension.Signature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReflectiveMethodTest {

  private static final Signature SIGNATURE = Signature.builder().build();

  private ReflectiveMethod reflectiveMethod;
  private Object receiver;

  @BeforeEach
  void setUp() throws Throwable {
    receiver = new Object();
    reflectiveMethod =
        new ReflectiveMethod(receiver, Object.class.getMethod("equals", Object.class), SIGNATURE);
  }

  @Test
  void getSignature() {
    assertThat(reflectiveMethod.getSignature()).isEqualTo(SIGNATURE);
  }

  @Test
  void invokeEqualWithReceiver_returnTrue() {
    assertThat((boolean) reflectiveMethod.invoke(receiver)).isTrue();
  }

  @Test
  void invokeEqualWithNewObject_returnFalse() {
    assertThat((boolean) reflectiveMethod.invoke(new Object())).isFalse();
  }
}
