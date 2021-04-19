import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;

import java.util.Iterator;

public class WindowFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<Long, Long>> input = env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, -2L), Tuple2.of(1L, 19L), Tuple2.of(2L, 10L), Tuple2.of(2L, 5L), Tuple2.of(2L, 23L));

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
                .countWindow(3)
                .aggregate(new AverageAggregate())
                .executeAndCollect();
        while (averageAggregate.hasNext()) {
            System.out.println(averageAggregate.next());
        }
    }

    public static class SumReduce implements ReduceFunction<Tuple2<Long, Long>> {
        @Override
        public Tuple2<Long, Long> reduce(Tuple2<Long, Long> value1, Tuple2<Long, Long> value2) throws Exception {
            return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
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
}

/*
SumReduce
(1,3)
(1,1)
(1,20)
(2,10)
(2,15)
(2,38)
AverageAggregate
6.666666666666667
12.666666666666666
 */