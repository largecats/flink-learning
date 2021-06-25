package join;

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

        DataStream<Integer> orangeStream = env.fromElements(
                0,2,3,4,5,7)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Integer>forBoundedOutOfOrderness(Duration.ofMillis(0))
                                .withTimestampAssigner((event, timestamp) -> event));
        DataStream<Integer> greenStream = env.fromElements(
                0,1,6,7)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Integer>forBoundedOutOfOrderness(Duration.ofMillis(0))
                                .withTimestampAssigner((event, timestamp) -> event));

        orangeStream
                .keyBy(x -> 0) // dummy key
                .intervalJoin(greenStream.keyBy(x -> 0))
                .between(Time.milliseconds(-2), Time.milliseconds(1)) // green elements need to be within (orangeElem.ts - 2, orangeElem.ts + 1)
                .process(new IntegerProcessFunction())
                .print();

        env.execute();
    }

    public static class IntegerProcessFunction extends ProcessJoinFunction<Integer, Integer, String> {
        @Override
        public void processElement(Integer first, Integer second, Context ctx, Collector<String> out) {
            out.collect(first + ", " + second);
        }
    }
}

/*
6> 0, 0
6> 2, 0
6> 0, 1
6> 2, 1
6> 3, 1
6> 5, 6
6> 7, 6
6> 7, 7
 */