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

public class SmallestReduceProcess {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<Long, Long, Long>> input = env.fromElements(Input.DATA)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<Long, Long, Long>>forBoundedOutOfOrderness(Duration.ofMillis(0))
                                .withTimestampAssigner((event, timestamp) -> event.f2) // 3rd column is timestamp
                );
        KeyedStream<Tuple3<Long, Long, Long>, Long> keyedInput = input.keyBy(x -> x.f0);

        // Get smallest event in window and window start time
        System.out.println("SmallestReduce + SmallestProcess");
        Iterator<Tuple2<Long, Long>> smallestReduceProcess = keyedInput
                .window(TumblingEventTimeWindows.of(Time.milliseconds(3)))
                .reduce(new SmallestReduce(), new SmallestProcess())
                .executeAndCollect();
        while (smallestReduceProcess.hasNext()) {
            System.out.println(smallestReduceProcess.next());
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
}

/*
SmallestReduce + SmallestProcess
(0,-2)
(3,5)
 */