package processFunction;

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

import common.utils.FixedInputGenerator;

public class CountWithTimeoutBatch {

    public static Tuple3<String, String, Long>[] input = new Tuple3[]{
//            Tuple3.of("a", "abase", 0L),
//            Tuple3.of("b", "bard", 500L),
//            Tuple3.of("a", "abate", 2500L),
//            Tuple3.of("b", "barrage", 3000L),
//            Tuple3.of("a", "abbreviate", 5000L),
//            Tuple3.of("b", "baroque", 5500L),
//            Tuple3.of("a", "abdicate", 7500L),
//            Tuple3.of("b", "barren", 8000L)
            Tuple3.of("a", "abase", 0L), // will trigger timeout for every two inputs
            Tuple3.of("b", "bard", 1250L),
            Tuple3.of("a", "abate", 2500L),
            Tuple3.of("b", "barrage", 3750L),
            Tuple3.of("a", "abbreviate", 5000L),
            Tuple3.of("b", "baroque", 6250L),
            Tuple3.of("a", "abdicate", 7500L),
            Tuple3.of("b", "barren", 8250L)
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStream<Tuple3<String, String, Long>> stream = env.fromElements(input) // can't simulate timeout
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy
//                                .<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofMillis(0))
//                                .withTimestampAssigner((event, timestamp) -> event.f2));
        DataStream<Tuple3<String, String, Long>> stream = env.addSource(new FixedInputGenerator().getGenerator(input));
        // Source
        // with
        // actual time gaps
        DataStream<Tuple2<String, Long>> result = stream
                .keyBy(value -> value.f0)
                .process(new CountWithTimeoutProcessFunction());

        result.print();

        env.execute();

    }
}

/*
Can simulate timeout using a source generated from batch:
element = (b,bard,500), timestamp = 500
element = (a,abase,0), timestamp = 0
element = (a,abate,2500), timestamp = 2500 // event time timer will only fire after watermark has passed the timer's fire time; so the following timers are only fired after the 2500ms event arrives
timestamp = 1500, result.lastModified = 500 // timer set for key b at 500ms for 1500ms
timestamp = 1000, result.lastModified = 2500 // timer set for key a at 0ms for 1000ms
result.key = b, result.count = 1 // since the 1000ms timer is not the latest, it won't trigger output by the logic
2> (b,1)
element = (b,barrage,3000), timestamp = 3000
timestamp = 4000, result.lastModified = 3000
result.key = b, result.count = 2
2> (b,2)
element = (b,baroque,5500), timestamp = 5500
timestamp = 6500, result.lastModified = 5500
result.key = b, result.count = 3
2> (b,3)
element = (b,barren,8000), timestamp = 8000
timestamp = 9000, result.lastModified = 8000
result.key = b, result.count = 4
2> (b,4)
element = (a,abbreviate,5000), timestamp = 5000
timestamp = 3500, result.lastModified = 5000
element = (a,abdicate,7500), timestamp = 7500
timestamp = 6000, result.lastModified = 7500
timestamp = 8500, result.lastModified = 7500
result.key = a, result.count = 4
6> (a,4)

Second set of input:
element = (a,abase,0), timestamp = 0
element = (b,bard,1250), timestamp = 1250
timestamp = 1000, result.lastModified = 0 // triggered as the 1250ms record arrives
timestamp = 2250, result.lastModified = 1250 // How is this timer triggered?
result.key = a, result.count = 1
result.key = b, result.count = 1
2> (b,1)
6> (a,1)
element = (a,abate,2500), timestamp = 2500
element = (b,barrage,3750), timestamp = 3750
timestamp = 4750, result.lastModified = 3750
result.key = b, result.count = 2
2> (b,2)
timestamp = 3500, result.lastModified = 2500
element = (b,baroque,6250), timestamp = 6250
result.key = a, result.count = 2
timestamp = 7250, result.lastModified = 6250
6> (a,2)
result.key = b, result.count = 3
element = (a,abbreviate,5000), timestamp = 5000
2> (b,3)
timestamp = 6000, result.lastModified = 5000
element = (b,barren,8250), timestamp = 8250
result.key = a, result.count = 3
timestamp = 9250, result.lastModified = 8250
6> (a,3)
result.key = b, result.count = 4
element = (a,abdicate,7500), timestamp = 7500
2> (b,4)
timestamp = 8500, result.lastModified = 7500
result.key = a, result.count = 4
6> (a,4)

Event time doesn't work that way with bounded input:
element = (b,bard,1250), timestamp = 1250
element = (a,abase,0), timestamp = 0
element = (a,abate,2500), timestamp = 2500
element = (b,barrage,3750), timestamp = 3750
element = (a,abbreviate,5000), timestamp = 5000
element = (b,baroque,6250), timestamp = 6250
element = (a,abdicate,7500), timestamp = 7500
element = (b,barren,8750), timestamp = 8750
timestamp = 1000, result.lastModified = 7500 // firing of timer set at 0ms
timestamp = 3500, result.lastModified = 7500 // firing of timer set at 2500ms
timestamp = 6000, result.lastModified = 7500
timestamp = 8500, result.lastModified = 7500
timestamp = 2250, result.lastModified = 8750
timestamp = 4750, result.lastModified = 8750
timestamp = 7250, result.lastModified = 8750
timestamp = 9750, result.lastModified = 8750
result.key = b, result.count = 4
result.key = a, result.count = 4
6> (a,4)
2> (b,4)
 */
