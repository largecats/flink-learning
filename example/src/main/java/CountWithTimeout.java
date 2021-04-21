import common.datatypes.TaxiRide;
import common.sources.TaxiRideGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction.Context;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction.OnTimerContext;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class CountWithTimeout {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<String, String, Long>> stream = env.fromElements( // how to simulate timeout?
                Tuple3.of("a", "abase", 0L),
                Tuple3.of("b", "bard", 0L),
                Tuple3.of("a", "abate", 70000L),
                Tuple3.of("b", "barrage", 70000L),
                Tuple3.of("a", "abbreviate", 140000L),
                Tuple3.of("b", "baroque", 140000L),
                Tuple3.of("a", "abdicate", 210000L),
                Tuple3.of("b", "barren", 210000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofMillis(0))
                                .withTimestampAssigner((event, timestamp) -> event.f2));
        DataStream<Tuple2<String, Long>> result = stream
                .keyBy(value -> value.f0)
                .process(new CountWithTimeoutProcessFunction());

        result.print();

        env.execute();

    }

    public static class CountWithTimestamp {
        public String key;
        public long count;
        public long lastModified;
    }

    public static class CountWithTimeoutProcessFunction extends KeyedProcessFunction<String, Tuple3<String, String, Long>, Tuple2<String, Long>> { // typo in Doc, the first argument should be String, not Tuple
        private ValueState<CountWithTimestamp> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
        }

        @Override
        public void processElement(
                Tuple3<String, String, Long> element,
                Context ctx,
                Collector<Tuple2<String, Long>> out) throws Exception {
            System.out.println("element = " + element + ", timestamp = " + ctx.timestamp());
            // retrieve current count
            CountWithTimestamp current = state.value();
            if (current == null) {
                current = new CountWithTimestamp();
                current.key = element.f0;
            }

            // update state's count
            current.count++;

            // set state's timestamp to the current record's assigned event time
            current.lastModified = ctx.timestamp();

            // write state back
            state.update(current);

            // set timer for 60s later
            ctx.timerService().registerEventTimeTimer(current.lastModified + 60000);
        }

        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext ctx,
                Collector<Tuple2<String, Long>> out) throws Exception {
            // get state for the key that scheduled this timer that is firing
            CountWithTimestamp result = state.value();

            // check if this timer is latest
            System.out.println("timestamp = " + timestamp + ", result.lastModified = " + result.lastModified);
            if (timestamp == result.lastModified + 60000) {
                // emit the state on timeout
                System.out.println("result.key = " + result.key + ", result.count = " + result.count);
                out.collect(new Tuple2<String, Long>(result.key, result.count));
            }
        }
    }
}

/*
element = (a,abase,0), timestamp = 0
element = (b,bard,0), timestamp = 0
element = (a,abate,70000), timestamp = 70000
element = (b,barrage,70000), timestamp = 70000
element = (a,abbreviate,140000), timestamp = 140000
element = (b,baroque,140000), timestamp = 140000
element = (a,abdicate,210000), timestamp = 210000
element = (b,barren,210000), timestamp = 210000
timestamp = 60000, result.lastModified = 210000 // firing of 60s timer set at 0, why result.lastModified is alrd 210000?
timestamp = 60000, result.lastModified = 210000
timestamp = 130000, result.lastModified = 210000 // firing of 60s timer set at 70000
timestamp = 130000, result.lastModified = 210000
timestamp = 200000, result.lastModified = 210000 // firing of 60s timer set at 140000
timestamp = 200000, result.lastModified = 210000
timestamp = 270000, result.lastModified = 210000 // firing of 60s timer set at 210000
timestamp = 270000, result.lastModified = 210000
result.key = a, result.count = 4
result.key = b, result.count = 4
2> (b,4) // Why only one result is emitted?
6> (a,4)
 */
