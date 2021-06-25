package window;

import common.datatypes.Rate;
import common.datatypes.RateCount;
import common.sources.BoundedRateGenerator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TumblingProcessingTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Rate> input = env.addSource(new BoundedRateGenerator());

        DataStream<RateCount> output = input
                .map(event -> new RateCount(event.id % 2, 1)) // using lambda
//                .map( // implementing MapFunction
//                        new MapFunction<Rate, RateCount>() {
//                            @Override
//                            public RateCount map(Rate value) throws Exception {
//                                return new RateCount(value.id % 10, 1);
//                            }
//                        }
//                )
                .keyBy(event -> event.id)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) // number of events per id per window
//                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10))) // total number of events per window
                .sum("count");

        output.print();

        env.execute();
    }
}
