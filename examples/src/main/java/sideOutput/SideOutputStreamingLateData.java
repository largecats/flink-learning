package sideOutput;

import common.utils.FixedInputGenerator;
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

public class SideOutputStreamingLateData {

    public static Tuple3<String, String, Long>[] input = new Tuple3[]{
            Tuple3.of("a", "4", 4L),
            Tuple3.of("a", "2", 2L), // late (comes after watermark passed 4)
            Tuple3.of("a", "7", 7L),
            Tuple3.of("a", "11", 11L),
            Tuple3.of("a", "9", 9L), // late (comes after watermark passed 11)
            Tuple3.of("a", "15", 15L),
            Tuple3.of("a", "12", 12L), // late (comes after watermark passed 15)
            Tuple3.of("a", "13", 13L) // late (comes after watermark passed 15)
    };

    public static OutputTag<Tuple3<String, String, Long>> late = new OutputTag<>("late") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStream<Tuple3<String, Integer, Long>> inputStream = env.fromElements(input) // can't simulate timeout
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy
//                                .<Tuple3<String, Integer, Long>>forBoundedOutOfOrderness(Duration.ofMillis(0))
//                                .withTimestampAssigner((event, timestamp) -> event.f2));
        DataStream<Tuple3<String, String, Long>> inputStream = env.addSource(new FixedInputGenerator().getGenerator(input));
        SingleOutputStreamOperator<Tuple3<String, String, Long>> result = inputStream
                .keyBy(x -> x.f0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(2)))
                .sideOutputLateData(late)
                .process(new FilterForPositive());

        DataStream<Tuple3<String, String, Long>> lateStream = result.getSideOutput(late);

        lateStream.print();

        env.execute();
    }

    public static class FilterForPositive extends ProcessWindowFunction<Tuple3<String, String, Long>,
                Tuple3<String, String, Long>, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Tuple3<String, String, Long>> elements
                , Collector<Tuple3<String, String, Long>> out) throws Exception {
            for (Tuple3<String, String, Long> in: elements) {
                if (Integer.parseInt(in.f1) > 0) {
                    out.collect(in);
                }
            }
        }
    }
}

/*
// Only print late data:
6> (a,2,2)
6> (a,9,9)
6> (a,12,12)
6> (a,13,13)
 */