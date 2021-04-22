package sideOutput;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class SideOutputLateData {

    public static OutputTag<Tuple3<String, Integer, Long>> late = new OutputTag<>("late") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String, Integer, Long>> input = env.fromElements(
                Tuple3.of("a", 4, 4L),
                Tuple3.of("a", 2, 2L),
                Tuple3.of("a", 7, 7L),
                Tuple3.of("a", 11, 11L),
                Tuple3.of("a", 9, 9L), // late
                Tuple3.of("a", 15, 15L),
                Tuple3.of("a", 12, 12L), // late
                Tuple3.of("a", 13, 13L) // late
            )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, Integer, Long>>forBoundedOutOfOrderness(Duration.ofMillis(0))
                                .withTimestampAssigner((event, timestamp) -> event.f2));
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> result = input
                .keyBy(x -> x.f0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(2)))
                .sideOutputLateData(late)
                .process(new FilterForPositive());

        DataStream<Tuple3<String, Integer, Long>> lateStream = result.getSideOutput(late);

        lateStream.print();

        env.execute();
    }

    public static class FilterForPositive extends ProcessWindowFunction<Tuple3<String, Integer, Long>,
                Tuple3<String, Integer, Long>, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Tuple3<String, Integer, Long>> elements
                , Collector<Tuple3<String, Integer, Long>> out) throws Exception {
            for (Tuple3<String, Integer, Long> in: elements) {
                if (in.f1 > 0) {
                    out.collect(in);
                }
            }
        }
    }
}
