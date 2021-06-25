package unittest;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.runtime.io.StreamOneInputProcessor;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperatorTest; // examples

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import processFunction.CountWithTimeoutProcessFunction;


import javax.print.DocFlavor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


import static org.junit.Assert.*; // has to be static

public class StatefulProcessFunctionTest {
    private KeyedOneInputStreamOperatorTestHarness<String, Tuple3<String, String, Long>, Tuple2<String, Long>> testHarness;
    private CountWithTimeoutProcessFunction statefulProcessFunction;

    @Before
    public void setupTestHarness() throws Exception {
        statefulProcessFunction = new CountWithTimeoutProcessFunction();

        testHarness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                statefulProcessFunction,
                x -> x.f0,
                Types.STRING);

        // open the test harness (will also call open() on RichFunctions)
        testHarness.open();
    }

    @Test
    public void testingStatefulProcessFunction() throws Exception {
        testHarness.processElement(Tuple3.of("a", "abase", 0L), 0L);
        testHarness.processWatermark(0L);
        assertEquals(
                Lists.newArrayList(),
                testHarness.extractOutputValues());
        testHarness.processElement(Tuple3.of("b", "bard", 1250L), 1250L);
        testHarness.processWatermark(1250L);
        testHarness.processElement(Tuple3.of("a", "abate", 2500L), 2500L);
        testHarness.processWatermark(2500L); // if without processWatermark, the output would be []
        assertEquals(
                Lists.newArrayList(
                        Tuple2.of("a", 1L),
                        Tuple2.of("b", 1L)
                ),
                testHarness.extractOutputValues()
	);
    }
}
