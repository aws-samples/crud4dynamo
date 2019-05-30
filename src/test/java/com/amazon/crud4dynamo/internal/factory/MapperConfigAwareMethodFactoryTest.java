package com.amazon.crud4dynamo.internal.factory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.amazon.crud4dynamo.annotation.MapperConfig;
import com.amazon.crud4dynamo.extension.Context;
import com.amazon.crud4dynamo.extension.Signature;
import com.amazon.crud4dynamo.extension.factory.AbstractMethodFactory;
import com.amazon.crud4dynamo.extension.method.AbstractMethod;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.ConsistentReads;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.SaveBehavior;
import java.lang.reflect.Method;
import java.util.function.Predicate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MapperConfigAwareMethodFactoryTest {

    public interface TestInterface {
        void withoutMapperConfigAnnotation();

        @MapperConfig
        void withDefaultMapperConfigAnnotation();

        @MapperConfig(saveBehavior = SaveBehavior.APPEND_SET, consistentReads = ConsistentReads.CONSISTENT)
        void withCustomMapperConfigAnnotation();
    }

    private ContextAwareFactory delegateFactory;
    private AbstractMethod delegateMethod;
    private MapperConfigAwareMethodFactory mapperConfigAwareMethodFactory;

    @BeforeEach
    void setUp() {
        delegateMethod = mock(AbstractMethod.class);
        delegateFactory = new ContextAwareFactory(delegateMethod);
        mapperConfigAwareMethodFactory = new MapperConfigAwareMethodFactory(delegateFactory);
    }

    @Test
    void methodWithoutMapperConfigAnnotation_delegate() throws Exception {
        final Context context = getContext("withoutMapperConfigAnnotation");

        assertThat(mapperConfigAwareMethodFactory.create(context)).isEqualTo(delegateMethod);
    }

    @Test
    void methodWithDefaultMapperConfigAnnotation() throws Exception {
        final Context context = getContext("withDefaultMapperConfigAnnotation");

        final AbstractMethod abstractMethod = mapperConfigAwareMethodFactory.create(context);

        assertThat(abstractMethod).isEqualTo(delegateMethod);
        final Context actualContext = delegateFactory.getContext();
        assertThat(actualContext).isEqualToIgnoringGivenFields(context, "mapperConfig");
        assertThat(actualContext.mapperConfig()).matches(newConfigPredicate(ConsistentReads.EVENTUAL, SaveBehavior.UPDATE));
    }

    @Test
    void methodWithCustomMapperConfigAnnotation() throws Exception {
        final Context context = getContext("withCustomMapperConfigAnnotation");

        final AbstractMethod abstractMethod = mapperConfigAwareMethodFactory.create(context);

        assertThat(abstractMethod).isEqualTo(delegateMethod);
        final Context actualContext = delegateFactory.getContext();
        assertThat(actualContext).isEqualToIgnoringGivenFields(context, "mapperConfig");
        assertThat(actualContext.mapperConfig()).matches(newConfigPredicate(ConsistentReads.CONSISTENT, SaveBehavior.APPEND_SET));
    }

    private Predicate<DynamoDBMapperConfig> newConfigPredicate(ConsistentReads consistent, SaveBehavior appendSet) {
        return config -> config.getConsistentReads().equals(consistent) && config.getSaveBehavior().equals(appendSet);
    }

    private Context getContext(final String methodName) throws NoSuchMethodException {
        final Method method = TestInterface.class.getMethod(methodName);
        return Context.builder()
                .interfaceType(TestInterface.class)
                .mapperConfig(
                        DynamoDBMapperConfig.builder()
                                .withSaveBehavior(SaveBehavior.CLOBBER)
                                .withConsistentReads(ConsistentReads.EVENTUAL)
                                .build())
                .method(method)
                .signature(Signature.resolve(method, TestInterface.class))
                .build();
    }

    private static class ContextAwareFactory implements AbstractMethodFactory {
        private Context context;
        private AbstractMethod delegateMethod;

        private ContextAwareFactory(final AbstractMethod delegateMethod) {
            this.delegateMethod = delegateMethod;
        }

        @Override
        public AbstractMethod create(Context context) {
            this.context = context;
            return delegateMethod;
        }

        private Context getContext() {
            return context;
        }
    }
}
