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
            // input 1
            Tuple3.of("a", "abase", 0L), // will trigger 4 outputs upon timeout for key b and 1 output for key a
            Tuple3.of("b", "bard", 500L),
            Tuple3.of("a", "abate", 2500L),
            Tuple3.of("b", "barrage", 3000L),
            Tuple3.of("a", "abbreviate", 5000L),
            Tuple3.of("b", "baroque", 5500L),
            Tuple3.of("a", "abdicate", 7500L),
            Tuple3.of("b", "barren", 8000L)
            // input 2
//            Tuple3.of("a", "abase", 0L), // will trigger 4 outputs upon timeout for both key a and key b
//            Tuple3.of("b", "bard", 1250L),
//            Tuple3.of("a", "abate", 2500L),
//            Tuple3.of("b", "barrage", 3750L),
//            Tuple3.of("a", "abbreviate", 5000L),
//            Tuple3.of("b", "baroque", 6250L),
//            Tuple3.of("a", "abdicate", 7500L),
//            Tuple3.of("b", "barren", 8250L)
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStream<Tuple3<String, String, Long>> stream = env.fromElements(input) // can't simulate timeout
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy
//                                .<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofMillis(0))
//                                .withTimestampAssigner((event, timestamp) -> event.f2));
        DataStream<Tuple3<String, String, Long>> stream = env.addSource(new FixedInputGenerator().getGenerator(input)); // Source with actual time gaps
        DataStream<Tuple2<String, Long>> result = stream
                .keyBy(value -> value.f0)
                .process(new CountWithTimeoutProcessFunction());

        result.print();

        env.execute();
    }
}

/*
"Unbounded" source generated from fixed batch input:
Input 1
2> (b,1)
2> (b,2)
2> (b,3)
2> (b,4)
6> (a,4)

Explanation:
(a, ..., 0L) arrives => timer [key a, 1000L] registered
(b, ..., 500L) arrives => timer [key b, 1500L] registered
(a, ..., 2500L) arrives => timer [key a, 3500L] registered
[key a, 1000L] timer fires => this is not the latest timer for key a
[key b, 1500L] timer fires => this is the latest timer for key b => output (b, 1)
(b, ..., 3000L) arrives => timer [key b, 4000L] registered
(a, 5000L) arrives => timer [key a, 6000L] registered
[key a, 3500L] timer fires => this is not the latest timer for key a
[key b, 4000L] timer fires => this is the latest timer for key b => output (b, 2)
(b, ..., 5500L) arrives => timer [key b, 6500L] registered
(a, ..., 7500L) arrives => timer [key a, 8500L] registered
[key a, 6000L] timer fires => this is not the latest timer for key a
[key b, 6500L] timer fires => this is the latest timer for key b => output (b, 3)
(b, ..., 8000L) arrives => timer [key b, 9000L] registered
(somehow) [key a, 8500L] timer fires => this is the latest timer for key a => output (a, 4)
(somehow) [key b, 9000L] timer fires => this is the latest timer for key b => output (b, 4)

input 2
2> (b,1)
6> (a,1)
2> (b,2)
6> (a,2)
2> (b,3)
6> (a,3)
2> (b,4)
6> (a,4)

Explanation:
(a, ..., 0L) arrives => timer [key a, 1000L] registered
(b, ..., 1250L) arrives => timer [key b, 2250L] registered
timer [key a, 1000L] fires => this is the latest timer for key a => output (a, 1)
(a, ..., 2500L) arrives => timer [key a, 3500L] registered
timer [key b, 2250L] fires => this is the latest timer for key b => output (b, 1)
(b, ..., 3750L) arrives => timer [key b, 4750L] registered
timer [key a, 3500L] fires => this is the latest timer for key a => output (a, 2)
(a, ..., 5000L) arrives => timer [key a, 6000L] registered
timer [key b, 4750L] fires => this is the latest timer for key b => output (b, 2)
(b, ..., 6250L) arrives => timer [key b, 7250L] registered
timer [key a, 6000L] fires => this is the latest timer for key a => output (a, 3)
(a, ..., 7500L) arrives => timer [key a, 8500L] registered
timer [key b, 7250L] fires => this is the latest timer for key b => output (b, 3)
(b, ..., 8000L) fires => timer [key b, 9000L] registered
timer [key a, 8500L] fires => this is the latest timer for key a => output (a, 4)
(somehow) timer [key b, 9000L] fires => this is the latest timer for key b => output (b, 4)


Bounded source:
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
