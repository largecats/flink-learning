import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class SlidingWindowJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Integer, Long>> orangeStream = env.fromElements(
                Tuple2.of(0, 0L), // integer, timestamp
                Tuple2.of(1, 1L),
                Tuple2.of(2, 2L),
                Tuple2.of(3, 3L),
                Tuple2.of(4, 4L))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<Integer, Long>>forBoundedOutOfOrderness(Duration.ofMillis(0))
                                .withTimestampAssigner((event, timestamp) -> event.f1));
        DataStream<Tuple2<Integer, Long>> greenStream = env.fromElements(
                Tuple2.of(0, 0L),
                Tuple2.of(3, 3L),
                Tuple2.of(4, 4L))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<Integer, Long>>forBoundedOutOfOrderness(Duration.ofMillis(0))
                                .withTimestampAssigner((event, timestamp) -> event.f1));

        orangeStream.join(greenStream)
                .where(x -> x.f0)
                .equalTo(x -> x.f0)
                .window(SlidingEventTimeWindows.of(Time.milliseconds(2), Time.milliseconds(1))) // 2ms windows slide by 1ms
                .apply(new IntegerJoinFunction())
                .print();

        env.execute();
    }

    public static class IntegerJoinFunction implements JoinFunction<Tuple2<Integer, Long>, Tuple2<Integer, Long>, String> {
        @Override
        public String join(Tuple2<Integer, Long> first, Tuple2<Integer, Long> second) {
            return first + ", " + second;
        }
    }
}

/*
6> (0,0), (0,0)
8> (3,3), (3,3)
1> (4,4), (4,4)
6> (0,0), (0,0)
1> (4,4), (4,4)
8> (3,3), (3,3)
 */