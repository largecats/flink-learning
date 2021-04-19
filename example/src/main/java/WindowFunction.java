import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class WindowFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<Long, Long>> input = env.fromElements(
                Tuple2.of(1L, 3L),
                Tuple2.of(1L, -2L),
                Tuple2.of(1L, 19L),
                Tuple2.of(2L, 10L),
                Tuple2.of(2L, 5L),
                Tuple2.of(2L, 23L)
        );

        KeyedStream<Tuple2<Long, Long>, Long> keyedInput = input.keyBy(x -> x.f0);

        System.out.println("SumReduce");
        Iterator<Tuple2<Long, Long>> sumReduce = keyedInput
                .reduce(new SumReduce())
                .executeAndCollect();
        while (sumReduce.hasNext()) {
            System.out.println(sumReduce.next());
        }

        System.out.println("AverageAggregate");
        Iterator<Double> averageAggregate = keyedInput
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(1))) // need some window; need to use milliseconds otherwise the output is empty, probably because this is batch stream?
//                .countWindow(3) // also can
                .aggregate(new AverageAggregate())
                .executeAndCollect();
        while (averageAggregate.hasNext()) {
            System.out.println(averageAggregate.next());
        }

        System.out.println("SumProcess");
        Iterator<String> sumProcess = keyedInput
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(1))) // need to use TimeWindow in SumProcess
//                .countWindow(3) // need to use GlobalWindow in SumProcess (so count window is a type of global window?)
                .process(new SumProcess())
                .executeAndCollect();
        while (sumProcess.hasNext()) {
            System.out.println(sumProcess.next());
        }

        // Get smallest event in window and window start time
        System.out.println("SmallestReduce + SmallestProcess");
        Iterator<Tuple2<Long, Long>> smallestReduceProcess = keyedInput
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(1)))
                .reduce(new SmallestReduce(), new SmallestProcess())
                .executeAndCollect();
        while (smallestReduceProcess.hasNext()) {
            System.out.println(smallestReduceProcess.next());
        }
    }

    public static class SumReduce implements ReduceFunction<Tuple2<Long, Long>> {
        @Override
        public Tuple2<Long, Long> reduce(Tuple2<Long, Long> e1, Tuple2<Long, Long> e2) throws Exception {
            return new Tuple2<>(e1.f0, e1.f1 + e2.f1);
        }
    }

    public static class AverageAggregate implements AggregateFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Double> {
        /*
        accumulator: (running sum, count)
        value: (key, value)
         */
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return new Tuple2<>(0L, 0L);
        }

        @Override
        public Tuple2<Long, Long> add(Tuple2<Long, Long> value, Tuple2<Long, Long> accumulator) {
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

    public static class SumProcess extends ProcessWindowFunction<Tuple2<Long, Long>, String, Long, TimeWindow> { // Must use GlobalWindow (so .countWindow() actually produces a GlobalWindow?
        @Override
        public void process(Long key, Context context, Iterable<Tuple2<Long, Long>> input, Collector<String> out) throws Exception {
            long sum = 0;
            for (Tuple2<Long, Long> in: input) {
                sum += in.f1;
            }
            out.collect("Window: " + context.window() + " sum: " + sum);
        }
    }

    public static class SmallestReduce implements ReduceFunction<Tuple2<Long, Long>> {
        @Override
        public Tuple2<Long, Long> reduce(Tuple2<Long, Long> e1, Tuple2<Long, Long> e2) throws Exception {
            return new Tuple2<>(e1.f0, e1.f1 > e2.f1? e2.f1: e1.f1);
        }
    }

    public static class SmallestProcess extends ProcessWindowFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Long, TimeWindow> {
        @Override
        public void process(Long key, Context context, Iterable<Tuple2<Long, Long>> minValues, Collector<Tuple2<Long, Long>> out) {
            Long min = minValues.iterator().next().f1;
            out.collect(new Tuple2<Long, Long>(context.window().getStart(), min));
        }
    }
}

/*
SumReduce
(2,10)
(2,15)
(2,38)
(1,3)
(1,1)
(1,20)
AverageAggregate
12.666666666666666
6.666666666666667
SumProcess
Window: GlobalWindow sum: 38
Window: GlobalWindow sum: 20
SmallestReduce + SmallestProcess
(1618820604034,3)
(1618820604035,10)
 */