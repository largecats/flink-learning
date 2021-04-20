import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class IntervalJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<Integer, Integer, Long>> orangeStream = env.fromElements(
                Tuple3.of(0, 0, 0L), // key, value, timestamp
                Tuple3.of(0, 2, 2L),
                Tuple3.of(0, 3, 3L),
                Tuple3.of(0, 4, 4L),
                Tuple3.of(0, 5, 5L),
                Tuple3.of(0, 7, 7L))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<Integer, Integer, Long>>forBoundedOutOfOrderness(Duration.ofMillis(0))
                                .withTimestampAssigner((event, timestamp) -> event.f1));
        DataStream<Tuple3<Integer, Integer, Long>> greenStream = env.fromElements(
                Tuple3.of(0, 0, 0L),
                Tuple3.of(0, 1, 1L),
                Tuple3.of(0, 6, 6L),
                Tuple3.of(0, 7, 7L))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<Integer, Integer, Long>>forBoundedOutOfOrderness(Duration.ofMillis(0))
                                .withTimestampAssigner((event, timestamp) -> event.f1));

        orangeStream
                .keyBy(x -> x.f0)
                .intervalJoin(greenStream.keyBy(x -> x.f0))
                .between(Time.milliseconds(-2), Time.milliseconds(1)) // green elements need to be within (orangeElem.ts - 2, orangeElem.ts + 1)
                .process(new IntegerProcessFunction())
                .print();

        env.execute();
    }

    public static class IntegerProcessFunction extends ProcessJoinFunction<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>, String> {
        @Override
        public void processElement(Tuple3<Integer, Integer, Long> first, Tuple3<Integer, Integer, Long> second, Context ctx, Collector<String> out) {
            out.collect(first + ", " + second);
        }
    }
}

/*
6> (0,0,0), (0,0,0)
6> (0,2,2), (0,0,0)
6> (0,0,0), (0,1,1)
6> (0,2,2), (0,1,1)
6> (0,3,3), (0,1,1)
6> (0,5,5), (0,6,6)
6> (0,7,7), (0,6,6)
6> (0,7,7), (0,7,7)
 */