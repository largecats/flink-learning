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

public class TaxiRideCountWithTimeout {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TaxiRide> stream = env.addSource(new TaxiRideGenerator()); // seems ok
        DataStream<Tuple2<Long, Long>> result = stream
                .keyBy(x -> x.driverId)
                .process(new TaxiRideCountWithTimeoutProcessFunction());

        result.print();

        env.execute();

    }

    public static class TaxiRideCountWithTimestamp {
        public long key;
        public long count;
        public long lastModified;
    }

    public static class TaxiRideCountWithTimeoutProcessFunction extends KeyedProcessFunction<Long, TaxiRide, Tuple2<Long, Long>> { // typo in Doc, the first argument should be String, not Tuple
        private ValueState<TaxiRideCountWithTimestamp> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", TaxiRideCountWithTimestamp.class));
        }

        @Override
        public void processElement(
                TaxiRide element,
                Context ctx,
                Collector<Tuple2<Long, Long>> out) throws Exception {
            System.out.println("element = " + element + ", timestamp = " + ctx.timestamp());
            // retrieve current count
            TaxiRideCountWithTimestamp current = state.value();
            if (current == null) {
                current = new TaxiRideCountWithTimestamp();
                current.key = element.driverId;
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
                Collector<Tuple2<Long, Long>> out) throws Exception {
            // get state for the key that scheduled this timer that is firing
            TaxiRideCountWithTimestamp result = state.value();

            // check if this timer is latest
            System.out.println("timestamp = " + timestamp + ", result.lastModified = " + result.lastModified);
            if (timestamp == result.lastModified + 60000) {
                // emit the state on timeout
                System.out.println("result.key = " + result.key + ", result.count = " + result.count);
                out.collect(new Tuple2<Long, Long>(result.key, result.count));
            }
        }
    }
}

/*
 */
