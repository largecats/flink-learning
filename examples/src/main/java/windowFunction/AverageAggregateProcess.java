package windowFunction;

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

public class AverageAggregateProcess {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<Long, Long, Long>> input = env.fromElements(Input.DATA)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<Long, Long, Long>>forBoundedOutOfOrderness(Duration.ofMillis(0))
                                .withTimestampAssigner((event, timestamp) -> event.f2) // 3rd column is timestamp
                );
        KeyedStream<Tuple3<Long, Long, Long>, Long> keyedInput = input.keyBy(x -> x.f0);

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

    public static class AverageProcess extends ProcessWindowFunction<Double, Tuple2<Long, Double>, Long, GlobalWindow> {
        @Override
        public void process(Long key, Context context, Iterable<Double> averages, Collector<Tuple2<Long, Double>> out) {
            Double average = averages.iterator().next();
            out.collect(new Tuple2<>(key, average));
        }
    }


}

/*
AverageAggregate + AverageProcess
(2,12.666666666666666)
(1,6.666666666666667)
 */