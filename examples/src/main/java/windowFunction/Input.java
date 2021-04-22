package windowFunction;

import org.apache.flink.api.java.tuple.*;

public class Input {
    public static final Tuple3<Long, Long, Long>[] DATA = new Tuple3[] {
            Tuple3.of(1L, 3L, 0L), // key, value, timestamp
            Tuple3.of(1L, -2L, 1L),
            Tuple3.of(1L, 19L, 2L),
            Tuple3.of(2L, 10L, 3L),
            Tuple3.of(2L, 5L, 4L),
            Tuple3.of(2L, 23L, 5L)
    };
}
