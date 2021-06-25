package join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class SlidingWindowJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> orangeStream = env.fromElements(
                0,1,2,3,4)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Integer>forBoundedOutOfOrderness(Duration.ofMillis(0))
                                .withTimestampAssigner((event, timestamp) -> event));
        DataStream<Integer> greenStream = env.fromElements(
                0,3,4)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Integer>forBoundedOutOfOrderness(Duration.ofMillis(0))
                                .withTimestampAssigner((event, timestamp) -> event));

        orangeStream.join(greenStream)
                .where(x -> 0) // dummy key
                .equalTo(x -> 0)
                .window(SlidingEventTimeWindows.of(Time.milliseconds(2), Time.milliseconds(1))) // 2ms windows slide by 1ms
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
6> 0, 0
6> 0, 0
6> 1, 0
6> 2, 3
6> 3, 3
6> 3, 3
6> 3, 4
6> 4, 3
6> 4, 4
6> 4, 4
 */