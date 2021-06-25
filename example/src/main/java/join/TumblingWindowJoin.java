package join;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.xml.crypto.Data;
import java.time.Duration;

public class TumblingWindowJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> orangeStream = env.fromElements(
                0, 1, 2, 3, 4, 5, 6, 7)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Integer>forBoundedOutOfOrderness(Duration.ofMillis(0))
                                .withTimestampAssigner((event, timestamp) -> event));
        DataStream<Integer> greenStream = env.fromElements(
                0,1,3,4)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Integer>forBoundedOutOfOrderness(Duration.ofMillis(0))
                        .withTimestampAssigner((event, timestamp) -> event));
        orangeStream.join(greenStream)
                .where(x -> 0) // dummy key
                .equalTo(x -> 0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(2))) // tumbling window of size 2ms
                .apply(new IntegerJoinFunction())
                .print(); // If output is empty, check if TumblingEventTimeWindows is mistakenly typed as TumblingProcessingTimeWindows

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
6> 0, 1
6> 1, 0
6> 1, 1
6> 2, 3
6> 3, 3
6> 4, 4
6> 5, 4
 */