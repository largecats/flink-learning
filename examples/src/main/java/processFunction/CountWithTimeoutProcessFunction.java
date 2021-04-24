package processFunction;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class CountWithTimeoutProcessFunction extends KeyedProcessFunction<String, Tuple3<String, String, Long>, Tuple2<String, Long>> { // typo in Doc, the first argument should be String, not Tuple
    public static class CountWithTimestamp {
        public String key;
        public long count;
        public long lastModified;
    }

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

        // set timer for 10s later
        ctx.timerService().registerEventTimeTimer(current.lastModified + 1000);
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
        if (timestamp == result.lastModified + 1000) {
            // emit the state on timeout
            System.out.println("result.key = " + result.key + ", result.count = " + result.count);
            out.collect(new Tuple2<String, Long>(result.key, result.count));
        }
    }
}