package join;

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

        DataStream<Integer> orangeStream = env.fromElements(
                1,2,5,6,8,9)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Integer>forBoundedOutOfOrderness(Duration.ofMillis(0))
                                .withTimestampAssigner((event, timestamp) -> event));
        DataStream<Integer> greenStream = env.fromElements(
                0,4,5)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Integer>forBoundedOutOfOrderness(Duration.ofMillis(0))
                                .withTimestampAssigner((event, timestamp) -> event));

        orangeStream.join(greenStream)
                .where(x -> 0) // dummy key
                .equalTo(x -> 0)
                .window(EventTimeSessionWindows.withGap(Time.milliseconds(1))) // 1ms-gap
                .apply(new IntegerJoinFunction())
                .print();

        env.execute();
    }

    public static class IntegerJoinFunction implements JoinFunction<Integer, Integer, String> {
        @Override
        public String join(Integer first, Integer second) {
            return first + ", " + second;
        }
    }
}

/*
6> 1, 0
6> 2, 0
6> 5, 4
6> 5, 5
6> 6, 4
6> 6, 5
 */