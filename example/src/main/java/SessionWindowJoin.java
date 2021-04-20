import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class SessionWindowJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<Integer, Integer, Long>> orangeStream = env.fromElements(
                Tuple3.of(0, 1, 1L), // key, value, timestamp
                Tuple3.of(0, 2, 2L),
                Tuple3.of(0, 5, 5L),
                Tuple3.of(0, 6, 6L),
                Tuple3.of(0, 8, 8L),
                Tuple3.of(0, 9, 9L))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<Integer, Integer, Long>>forBoundedOutOfOrderness(Duration.ofMillis(0))
                                .withTimestampAssigner((event, timestamp) -> event.f1));
        DataStream<Tuple3<Integer, Integer, Long>> greenStream = env.fromElements(
                Tuple3.of(0, 0, 0L),
                Tuple3.of(0, 4, 4L),
                Tuple3.of(0, 5, 5L))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<Integer, Integer, Long>>forBoundedOutOfOrderness(Duration.ofMillis(0))
                                .withTimestampAssigner((event, timestamp) -> event.f1));

        orangeStream.join(greenStream)
                .where(x -> x.f0)
                .equalTo(x -> x.f0)
                .window(EventTimeSessionWindows.withGap(Time.milliseconds(1))) // 1ms-gap
                .apply(new IntegerJoinFunction())
                .print();

        env.execute();
    }

    public static class IntegerJoinFunction implements JoinFunction<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>, String> {
        @Override
        public String join(Tuple3<Integer, Integer, Long> first, Tuple3<Integer, Integer, Long> second) {
            return first + ", " + second;
        }
    }
}

/*
6> (0,1,1), (0,0,0)
6> (0,2,2), (0,0,0)
6> (0,5,5), (0,4,4)
6> (0,5,5), (0,5,5)
6> (0,6,6), (0,4,4)
6> (0,6,6), (0,5,5)
 */