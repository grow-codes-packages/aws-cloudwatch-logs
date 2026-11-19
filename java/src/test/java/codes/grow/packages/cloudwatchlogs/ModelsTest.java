package codes.grow.packages.cloudwatchlogs;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class ModelsTest {
  static final List<Class<?>> OPTIONS =
      List.of(
          EnsureLogGroupOptions.class,
          DescribeLogGroupsOptions.class,
          DescribeLogStreamsOptions.class,
          PutLogEventsOptions.class,
          FilterLogEventsOptions.class,
          GetLogEventsOptions.class,
          QueryOptions.class,
          DescribeQueriesOptions.class,
          LiveTailRequest.class,
          LiveTailOptions.class);

  static final List<Class<?>> VALUES =
      List.of(
          LogGroupInfo.class,
          LogStreamInfo.class,
          FilteredLogEvent.class,
          StreamLogEvent.class,
          RejectedLogEventsInfo.class,
          PutLogEventsResult.class,
          QueryStatistics.class,
          QueryResults.class,
          QueryInfo.class,
          LiveTailEvent.class,
          LiveTailUpdate.class,
          EnsureLogGroupResult.class,
          EnsureLogStreamResult.class,
          FilterLogEventsPage.class);

  static Object sample(Class<?> type) {
    if (type == String.class) {
      return "v";
    }
    if (type == Integer.class || type == int.class) {
      return 7;
    }
    if (type == Boolean.class || type == boolean.class) {
      return true;
    }
    if (type == double.class) {
      return 1.5;
    }
    if (type == Instant.class) {
      return Instant.EPOCH;
    }
    if (type == Duration.class) {
      return Duration.ofSeconds(2);
    }
    if (type == List.class) {
      return List.of("a");
    }
    if (type == Map.class) {
      return Map.of("k", "v");
    }
    if (type == Consumer.class) {
      return (Consumer<Object>) x -> {};
    }
    if (type.isEnum()) {
      return type.getEnumConstants()[0];
    }
    return null;
  }

  @Test
  void optionBuildersRoundTrip() throws Exception {
    for (Class<?> c : OPTIONS) {
      Object defaults = c.getMethod("defaults").invoke(null);
      assertThat(defaults).as(c.getSimpleName()).isNotNull();
      Object builder = c.getMethod("builder").invoke(null);
      for (Method m : builder.getClass().getDeclaredMethods()) {
        if (m.getParameterCount() == 1 && m.getReturnType() == builder.getClass()) {
          m.invoke(builder, sample(m.getParameterTypes()[0]));
        }
      }
      Object built = builder.getClass().getMethod("build").invoke(builder);
      Object copy = c.getMethod("toBuilder").invoke(built);
      Object rebuilt = copy.getClass().getMethod("build").invoke(copy);
      assertThat(rebuilt).as(c.getSimpleName()).isEqualTo(built);
      for (RecordComponent rc : c.getRecordComponents()) {
        assertThat(rc.getAccessor().invoke(built))
            .as(c.getSimpleName() + "." + rc.getName())
            .isNotNull();
      }
    }
    QueryOptions q = QueryOptions.defaults();
    assertThat(q.timeout()).isEqualTo(Duration.ofSeconds(60));
    assertThat(q.pollInterval()).isEqualTo(Duration.ofSeconds(1));
    assertThat(q.retryOnQueryLimit()).isEqualTo(3);
    assertThat(EnsureLogGroupOptions.defaults().tags()).isEmpty();
    assertThat(LiveTailRequest.defaults().groups()).isNull();
  }

  @Test
  void valueRecordsNormalizeAbsentValues() throws Exception {
    for (Class<?> c : VALUES) {
      RecordComponent[] rcs = c.getRecordComponents();
      Class<?>[] types = new Class<?>[rcs.length];
      Object[] nulls = new Object[rcs.length];
      for (int i = 0; i < rcs.length; i++) {
        types[i] = rcs[i].getType();
        nulls[i] = types[i].isPrimitive() ? sample(types[i]) : null;
        if (types[i] == QueryStatus.class) {
          nulls[i] = QueryStatus.COMPLETE;
        }
      }
      Constructor<?> ctor = c.getDeclaredConstructor(types);
      Object v = ctor.newInstance(nulls);
      for (RecordComponent rc : rcs) {
        Object value = rc.getAccessor().invoke(v);
        Class<?> t = rc.getType();
        if (t == Optional.class) {
          assertThat((Optional<?>) value).isEmpty();
        } else if (t == OptionalInt.class) {
          assertThat((OptionalInt) value).isEmpty();
        } else if (t == OptionalLong.class) {
          assertThat((OptionalLong) value).isEmpty();
        } else if (t == List.class) {
          assertThat((List<?>) value).isEmpty();
        }
      }
      assertThat(v.toString()).isNotEmpty();
    }
  }

  @Test
  void enums() {
    assertThat(QueryStatus.parse("complete")).isEqualTo(QueryStatus.COMPLETE);
    assertThat(QueryStatus.parse(null)).isEqualTo(QueryStatus.UNKNOWN);
    assertThat(QueryStatus.parse("Nope")).isEqualTo(QueryStatus.UNKNOWN);
    assertThat(QueryStatus.RUNNING.isTerminal()).isFalse();
    assertThat(QueryStatus.SCHEDULED.isTerminal()).isFalse();
    assertThat(QueryStatus.FAILED.isTerminal()).isTrue();
    assertThat(QueryStatus.CANCELLED.wireValue()).isEqualTo("Cancelled");
    assertThat(LogStreamOrder.LOG_STREAM_NAME.wireValue()).isEqualTo("LogStreamName");
  }
}
