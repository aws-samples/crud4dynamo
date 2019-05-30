package com.amazon.crud4dynamo.extension;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazon.crud4dynamo.annotation.Cached;
import com.amazon.crud4dynamo.annotation.Query;
import com.amazon.crud4dynamo.annotation.transaction.ConditionCheck;
import com.amazon.crud4dynamo.annotation.transaction.ConditionChecks;
import java.lang.reflect.Method;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class SignatureTest {

    private interface ParameterizedFunctionInterface extends Function<String, Integer> {
        @Cached
        void aMethod();

        @ConditionCheck(tableClass = Class.class, keyExpression = "A", conditionExpression = "")
        @ConditionChecks({@ConditionCheck(tableClass = Class.class, keyExpression = "B", conditionExpression = "")})
        void bMethod();
    }

    @Test
    void resolveGenericTypes() throws Exception {
        final Method applyMethod = Function.class.getMethod("apply", Object.class);

        final Signature sig = Signature.resolve(applyMethod, ParameterizedFunctionInterface.class);

        assertThat(sig.string()).isEqualTo("java.lang.Integer apply(java.lang.String)");
    }

    @Test
    void getAnnotation() throws Exception {
        final Method aMethod = ParameterizedFunctionInterface.class.getMethod("aMethod");

        final Signature sig = Signature.resolve(aMethod, ParameterizedFunctionInterface.class);

        assertThat(sig.getAnnotation(Query.class)).isEmpty();
        assertThat(sig.getAnnotation(Cached.class)).isPresent();
    }

    @Test
    void getAnnotationsByType() throws Exception {
        final Method aMethod = ParameterizedFunctionInterface.class.getMethod("bMethod");

        final Signature sig = Signature.resolve(aMethod, ParameterizedFunctionInterface.class);

        assertThat(sig.getAnnotationsByType(ConditionCheck.class)).hasSize(2);
        assertThat(sig.getAnnotationsByType(ConditionChecks.class)).hasSize(1);
    }
}
