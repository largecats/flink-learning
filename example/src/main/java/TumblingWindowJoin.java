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

        DataStream<Tuple3<Integer, Integer, Long>> orangeStream = env.fromElements(
                Tuple3.of(0, 0, 0L), // key, value, timestamp
                Tuple3.of(0, 1, 1L),
                Tuple3.of(0, 2, 2L),
                Tuple3.of(0, 3, 3L),
                Tuple3.of(0, 4, 4L),
                Tuple3.of(0, 5, 5L),
                Tuple3.of(0, 6, 6L),
                Tuple3.of(0, 7, 7L))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<Integer, Integer, Long>>forBoundedOutOfOrderness(Duration.ofMillis(0))
                                .withTimestampAssigner((event, timestamp) -> event.f1));
        DataStream<Tuple3<Integer, Integer, Long>> greenStream = env.fromElements(
                Tuple3.of(0, 0, 0L),
                Tuple3.of(0, 1, 1L),
                Tuple3.of(0, 3, 3L),
                Tuple3.of(0, 4, 4L))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<Integer, Integer, Long>>forBoundedOutOfOrderness(Duration.ofMillis(0))
                        .withTimestampAssigner((event, timestamp) -> event.f1));
        orangeStream.join(greenStream)
                .where(x -> x.f0)
                .equalTo(x -> x.f0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(2))) // tumbling window of size 2ms
                .apply(new IntegerJoinFunction())
                .print(); // If output is empty, check if TumblingEventTimeWindows is mistakenly typed as TumblingProcessingTimeWindows

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
6> (0,0,0), (0,0,0)
6> (0,0,0), (0,1,1)
6> (0,1,1), (0,0,0)
6> (0,1,1), (0,1,1)
6> (0,2,2), (0,3,3)
6> (0,3,3), (0,3,3)
6> (0,4,4), (0,4,4)
6> (0,5,5), (0,4,4)
 */