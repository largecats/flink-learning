import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.xml.crypto.Data;
import java.time.Duration;

public class TumblingWindowJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<Integer, Long>> orangeStream = env.fromElements(
                Tuple2.of(0, 0L), // integer, timestamp
                Tuple2.of(1, 1L),
                Tuple2.of(2, 2L),
                Tuple2.of(3, 3L),
                Tuple2.of(4, 4L),
                Tuple2.of(5, 5L),
                Tuple2.of(6, 6L),
                Tuple2.of(7, 7L));
        orangeStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<Integer, Long>>forBoundedOutOfOrderness(Duration.ofMillis(0))
                                .withTimestampAssigner((event, timestamp) -> event.f1));
        DataStreamSource<Tuple2<Integer, Long>> greenStream = env.fromElements(
                Tuple2.of(0, 0L),
                Tuple2.of(1, 1L),
                Tuple2.of(3, 3L),
                Tuple2.of(4, 4L)
        );
        greenStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<Integer, Long>>forBoundedOutOfOrderness(Duration.ofMillis(0))
                        .withTimestampAssigner((event, timestamp) -> event.f1));
        orangeStream.join(greenStream)
                .where(x -> x.f0)
                .equalTo(x -> x.f0)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(2))) // tumbling window of size 2ms
                .apply(new IntegerJoinFunction())
                .print();

        env.execute();

    }

    public static class IntegerJoinFunction implements JoinFunction<Tuple2<Integer, Long>, Tuple2<Integer, Long>, String> {
        @Override
        public String join(Tuple2<Integer, Long> first, Tuple2<Integer, Long> second) {
            System.out.println(first + ", " + second);
            return first + ", " + second;
        }
    }
}
