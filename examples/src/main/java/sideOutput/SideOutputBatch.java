package sideOutput;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Collector;

public class SideOutputBatch {
    private static final OutputTag<Tuple2<String, Integer>> negative = new OutputTag<Tuple2<String, Integer>>("negative") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> input = env.fromElements(
                Tuple2.of("a", -2),
                Tuple2.of("a", 4),
                Tuple2.of("a", 0),
                Tuple2.of("b", -9),
                Tuple2.of("b", 5),
                Tuple2.of("b", 6),
                Tuple2.of("c", 0),
                Tuple2.of("c", 7));

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = input
                .keyBy(x -> x.f0)
                .process(new FilterForPositive());

        result.getSideOutput(negative).print();

        env.execute();
    }

    public static class FilterForPositive extends KeyedProcessFunction<String, Tuple2<String, Integer>,
            Tuple2<String, Integer>> {
        @Override
        public void processElement(Tuple2<String, Integer> element, Context ctx,
                                   Collector<Tuple2<String, Integer>> out) throws Exception {
            if (element.f1 < 0) {
                ctx.output(negative, element);
            } else {
                out.collect(element);
            }
        }
    }
}

/*
6> (a,-2)
2> (b,-9)
 */
