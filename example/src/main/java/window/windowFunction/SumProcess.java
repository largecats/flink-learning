package window.windowFunction;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;
import java.util.Iterator;
import java.time.Duration;

public class SumProcess {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<Long, Long, Long>> input = env.fromElements(Input.data)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<Long, Long, Long>>forBoundedOutOfOrderness(Duration.ofMillis(0))
                                .withTimestampAssigner((event, timestamp) -> event.f2) // 3rd column is timestamp
                );
        KeyedStream<Tuple3<Long, Long, Long>, Long> keyedInput = input.keyBy(x -> x.f0);

        System.out.println("SumProcess");
        Iterator<String> sumProcess = keyedInput
                .window(TumblingEventTimeWindows.of(Time.milliseconds(3))) // if use this line, need to use
                // TimeWindow in SumProcess
//                .countWindow(3) // if use this line, need to use GlobalWindow in SumProcess (so count window is a type of global window?)
                .process(new SumProcessWindowFunction())
                .executeAndCollect();
        while (sumProcess.hasNext()) {
            System.out.println(sumProcess.next());
        }
    }

        public static class SumProcessWindowFunction extends ProcessWindowFunction<Tuple3<Long, Long, Long>, String,
                Long, TimeWindow> {
//    public static class SumProcessWindowFunction extends ProcessWindowFunction<Tuple3<Long, Long, Long>, String,
//    Long, GlobalWindow> {
        @Override
        public void process(Long key, Context context, Iterable<Tuple3<Long, Long, Long>> input, Collector<String> out) throws Exception {
            long sum = 0;
            for (Tuple3<Long, Long, Long> in: input) {
                sum += in.f1;
            }
            out.collect("Window: " + context.window() + " sum: " + sum);
        }
    }
}

/*
Using time window:
SumProcess
Window: TimeWindow{start=3, end=6} sum: 38
Window: TimeWindow{start=0, end=3} sum: 20

Using count window:
SumProcess
Window: GlobalWindow sum: 38
Window: GlobalWindow sum: 20
 */