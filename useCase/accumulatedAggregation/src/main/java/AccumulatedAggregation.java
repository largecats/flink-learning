import common.sources.RateGenerator;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Directly run in IDE.
 */
public class AccumulatedAggregation {

    public static long partitionInterval = 5 * 60 * 1000L; // In the actual DAU case, this is 1 day
    public static long accumulateInterval = 60 * 1000L; // Output an updated DAU count every 60s
    public static long timeoutDuration = 2 * 60 * 1000L; // At the last accumulateInterval of each day, wait for some time to make sure all late data has arrived before outputting the final DAU count

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Event> events = env.addSource(new RateGenerator()).map(x -> new Event(x.id.toString(), new Timestamp(x.timestamp)));
        DataStream<DailyAppend> dailyAppend = events
                .keyBy(event -> event.timestamp.getTime()/partitionInterval * partitionInterval)
                .process(new AccumulatedAggregationProcessFunction());

        // Can't tell Collector<> type, so use .returns() to supply explicit type information
        dailyAppend.map(x -> new Tuple2<Timestamp, Long>(x.time, x.accumulatedDistinct)).returns(Types.TUPLE(Types.SQL_TIMESTAMP, Types.LONG)).print();

        env.execute();
    }

    public static class AccumulatedAggregationProcessFunction extends KeyedProcessFunction<Long, Event, DailyAppend> {

        private ValueState<DailyInfo> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", DailyInfo.class));
        }

        @Override
        public void processElement(Event event, Context ctx, Collector<DailyAppend> out) throws Exception {
            DailyInfo current = state.value();
            if (current == null) {
                current = new DailyInfo();
                current.users = new HashMap<String, Long>();
                current.users.put(event.uid, event.timestamp.getTime());
                current.accumulateStart = ctx.timestamp();
                current.lastModified = ctx.timestamp();
                state.update(current);
                // Processing time
//                ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + accumulateInterval);
                // Event time
                ctx.timerService().registerEventTimeTimer((state.value().accumulateStart + accumulateInterval)/1000*1000);
            } else {
                if (current.users.containsKey(event.uid)) {
                    current.users.put(event.uid, Math.min(current.users.get(event.uid), event.timestamp.getTime()));
                } else {
                    current.users.put(event.uid, event.timestamp.getTime());
                }
                current.lastModified = ctx.timestamp();
                state.update(current);
            }
            // Event time with timeout
            ctx.timerService().registerEventTimeTimer((state.value().lastModified + timeoutDuration)/1000*1000);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<DailyAppend> out) throws Exception {
            // Processing time
//            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + accumulateInterval);
            // Event time
//            state.value().accumulateStart = timestamp;
//            ctx.timerService().registerEventTimeTimer(state.value().accumulateStart + accumulateInterval);
//            out.collect(new DailyAppend(new Timestamp(timestamp), (long) state.value().users.size()));
            // Event time with timeout
            if (state.value() != null) {
                if (timestamp == (state.value().lastModified + timeoutDuration)/1000*1000) { // Timer to output final count after timeout
                    DailyInfo result = state.value();
                    state.clear(); // Remove state after timeout
//                    ctx.timerService().deleteEventTimeTimer((result.accumulateStart + accumulateInterval)/1000*1000); // Remove the next accumulate timer (or check if state is null instead)
                    out.collect(new DailyAppend(new Timestamp(timestamp), (long) result.users.size()));
                } else if (timestamp == (state.value().accumulateStart + accumulateInterval)/1000*1000) { // Timer to output accumulate count
                    state.value().accumulateStart = timestamp/1000*1000;
                    ctx.timerService().registerEventTimeTimer((state.value().accumulateStart + accumulateInterval)/1000*1000);
                    out.collect(new DailyAppend(new Timestamp(timestamp), (long) state.value().users.size()));
                }
            }
        }
    }
}

/*
Processing time
6> (2021-05-02 23:40:02.746,61)
6> (2021-05-02 23:41:02.754,120)
6> (2021-05-02 23:42:02.762,180)
...

Event time
3> (2021-05-03 14:56:36.824,61)
3> (2021-05-03 14:57:36.824,121)
3> (2021-05-03 14:58:36.824,181)
...

Event time with timeout (1min)
Note that the last accumulate_interval of the first partition_interval may be incomplete (i.e., < 300) if RateGenerator.START is .now()
4> (2021-05-03 16:11:46.0,61)
4> (2021-05-03 16:12:46.0,121)
4> (2021-05-03 16:13:46.0,181)
4> (2021-05-03 16:14:46.0,241)
4> (2021-05-03 16:15:46.0,254)
4> (2021-05-03 16:15:59.0,254)
3> (2021-05-03 16:31:00.0,61)
3> (2021-05-03 16:32:00.0,121)
3> (2021-05-03 16:33:00.0,181)
3> (2021-05-03 16:34:00.0,241)
3> (2021-05-03 16:35:00.0,300)
2> (2021-05-03 16:36:00.0,61)
...

Event time with timeout (2min)
8> (2021-05-03 16:41:24.0,61)
8> (2021-05-03 16:42:24.0,121)
8> (2021-05-03 16:43:24.0,181)
8> (2021-05-03 16:44:24.0,241)
8> (2021-05-03 16:45:24.0,276)
1> (2021-05-03 16:46:00.0,61)
8> (2021-05-03 16:46:24.0,276)
8> (2021-05-03 16:46:59.0,276)
1> (2021-05-03 16:47:00.0,121)
1> (2021-05-03 16:48:00.0,181)
1> (2021-05-03 16:49:00.0,241)
1> (2021-05-03 16:50:00.0,300)
1> (2021-05-03 16:51:00.0,300)
6> (2021-05-03 16:51:00.0,61)
1> (2021-05-03 16:51:59.0,300)
6> (2021-05-03 16:52:00.0,121)
6> (2021-05-03 16:53:00.0,181)
6> (2021-05-03 16:54:00.0,241)
6> (2021-05-03 16:55:00.0,300)
6> (2021-05-03 16:56:00.0,300)
2> (2021-05-03 16:56:00.0,61)
...
 */