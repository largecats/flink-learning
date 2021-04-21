import org.apache.commons.net.ntp.TimeStamp;
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

public class WindowFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<Long, Long, Long>> input = env.fromElements(
                Tuple3.of(1L, 3L, 0L), // key, value, timestamp
                Tuple3.of(1L, -2L, 1L),
                Tuple3.of(1L, 19L, 2L),
                Tuple3.of(2L, 10L, 3L),
                Tuple3.of(2L, 5L, 4L),
                Tuple3.of(2L, 23L, 5L))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<Long, Long, Long>>forBoundedOutOfOrderness(Duration.ofMillis(0))
                        .withTimestampAssigner((event, timestamp) -> event.f2) // 3rd column is timestamp
                );
        KeyedStream<Tuple3<Long, Long, Long>, Long> keyedInput = input.keyBy(x -> x.f0);

        System.out.println("SumReduce");
        Iterator<Tuple3<Long, Long, Long>> sumReduce = keyedInput
                .reduce(new SumReduce())
                .executeAndCollect();
        while (sumReduce.hasNext()) {
            System.out.println(sumReduce.next());
        }

        System.out.println("AverageAggregate");
        Iterator<Double> averageAggregate = keyedInput
                .countWindow(3)
                .aggregate(new AverageAggregate())
                .executeAndCollect();
        while (averageAggregate.hasNext()) {
            System.out.println(averageAggregate.next());
        }

        System.out.println("SumProcess");
        Iterator<String> sumProcess = keyedInput
//                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(2))) // if use this line, need to use TimeWindow in SumProcess
                .countWindow(3) // if use this line, need to use GlobalWindow in SumProcess (so count window is a type of global window?)
                .process(new SumProcess())
                .executeAndCollect();
        while (sumProcess.hasNext()) {
            System.out.println(sumProcess.next());
        }

        // Get smallest event in window and window start time
        System.out.println("SmallestReduce + SmallestProcess");
        Iterator<Tuple2<Long, Long>> smallestReduceProcess = keyedInput
                .window(TumblingEventTimeWindows.of(Time.milliseconds(3)))
                .reduce(new SmallestReduce(), new SmallestProcess())
                .executeAndCollect();
        while (smallestReduceProcess.hasNext()) {
            System.out.println(smallestReduceProcess.next());
        }

        // Get window average along with key
        System.out.println("AverageAggregate + AverageProcess");
        Iterator<Tuple2<Long, Double>> averageAggregateProcess = keyedInput
                .countWindow(3)
                .aggregate(new AverageAggregate(), new AverageProcess())
                .executeAndCollect();
        while (averageAggregateProcess.hasNext()) {
            System.out.println(averageAggregateProcess.next());
        }
    }

    public static class SumReduce implements ReduceFunction<Tuple3<Long, Long, Long>> {
        @Override
        public Tuple3<Long, Long, Long> reduce(Tuple3<Long, Long, Long> e1, Tuple3<Long, Long, Long> e2) throws Exception {
            return new Tuple3<>(e1.f0, e1.f1 + e2.f1, e1.f2 < e2.f2? e2.f2: e1.f2);
        }
    }

    public static class AverageAggregate implements AggregateFunction<Tuple3<Long, Long, Long>, Tuple2<Long, Long>, Double> {
        /*
        accumulator: (running sum, count)
        value: (key, value)
         */
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return new Tuple2<>(0L, 0L);
        }

        @Override
        public Tuple2<Long, Long> add(Tuple3<Long, Long, Long> value, Tuple2<Long, Long> accumulator) {
            return new Tuple2<>(accumulator.f0 + value.f1, accumulator.f1 + 1L);
        }

        @Override
        public Double getResult(Tuple2<Long, Long> accumulator) {
            return ((double) accumulator.f0) / accumulator.f1;
        }

        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }

//    public static class SumProcess extends ProcessWindowFunction<Tuple3<Long, Long, Long>, String, Long, TimeWindow> {
    public static class SumProcess extends ProcessWindowFunction<Tuple3<Long, Long, Long>, String, Long, GlobalWindow> {
        @Override
        public void process(Long key, Context context, Iterable<Tuple3<Long, Long, Long>> input, Collector<String> out) throws Exception {
            long sum = 0;
            for (Tuple3<Long, Long, Long> in: input) {
                sum += in.f1;
            }
            out.collect("Window: " + context.window() + " sum: " + sum);
        }
    }

    public static class SmallestReduce implements ReduceFunction<Tuple3<Long, Long, Long>> {
        @Override
        public Tuple3<Long, Long, Long> reduce(Tuple3<Long, Long, Long> e1, Tuple3<Long, Long, Long> e2) throws Exception {
            return e1.f1 < e2.f1? e1: e2;
        }
    }

    public static class SmallestProcess extends ProcessWindowFunction<Tuple3<Long, Long, Long>, Tuple2<Long, Long>, Long, TimeWindow> {
        @Override
        public void process(Long key, Context context, Iterable<Tuple3<Long, Long, Long>> minValues, Collector<Tuple2<Long, Long>> out) {
            Long min = minValues.iterator().next().f1;
            out.collect(new Tuple2<Long, Long>(context.window().getStart(), min));
        }
    }

    public static class AverageProcess extends ProcessWindowFunction<Double, Tuple2<Long, Double>, Long, GlobalWindow> {
        @Override
        public void process(Long key, Context context, Iterable<Double> averages, Collector<Tuple2<Long, Double>> out) {
            Double average = averages.iterator().next();
            out.collect(new Tuple2<>(key, average));
        }
    }


}

/*
SumReduce
(1,3,0)
(1,1,1)
(1,20,2)
(2,10,3)
(2,15,4)
(2,38,5)
AverageAggregate
12.666666666666666
6.666666666666667
SumProcess
Window: GlobalWindow sum: 38
Window: GlobalWindow sum: 20
SmallestReduce + SmallestProcess
(0,-2)
(3,5)
AverageAggregate + AverageProcess
(2,12.666666666666666)
(1,6.666666666666667)
 */