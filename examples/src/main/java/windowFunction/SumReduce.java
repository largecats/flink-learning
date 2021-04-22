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

public class SumReduce {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<Long, Long, Long>> input = env.fromElements(Input.DATA)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<Long, Long, Long>>forBoundedOutOfOrderness(Duration.ofMillis(0))
                                .withTimestampAssigner((event, timestamp) -> event.f2) // 3rd column is timestamp
                );
        KeyedStream<Tuple3<Long, Long, Long>, Long> keyedInput = input.keyBy(x -> x.f0);

//        keyedInput
//                .reduce(new SumReduceFunction())
//                .print();
//        env.execute();

        // If there are other outputs, can use Iterator to collect results
        System.out.println("SumReduce");
        Iterator<Tuple3<Long, Long, Long>> sumReduce = keyedInput
                .reduce(new SumReduceFunction())
                .executeAndCollect();
        while (sumReduce.hasNext()) {
            System.out.println(sumReduce.next());
        }

    }

    public static class SumReduceFunction implements ReduceFunction<Tuple3<Long, Long, Long>> {
        @Override
        public Tuple3<Long, Long, Long> reduce(Tuple3<Long, Long, Long> e1, Tuple3<Long, Long, Long> e2) throws Exception {
            return new Tuple3<>(e1.f0, e1.f1 + e2.f1, e1.f2 < e2.f2? e2.f2: e1.f2);
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

6> (1,3,0)
8> (2,10,3)
6> (1,1,1)
8> (2,15,4)
6> (1,20,2)
8> (2,38,5)
 */