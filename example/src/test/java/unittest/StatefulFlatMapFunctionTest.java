package unittest;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.*; // has to be static

public class StatefulFlatMapFunctionTest {
    private KeyedOneInputStreamOperatorTestHarness<String, String, Long> testHarness;
    private StatefulFlatMapFunction statefulFlatMapFunction;

    @Before
    public void setupTestHarness() throws Exception {

        //instantiate user-defined function
        statefulFlatMapFunction = new StatefulFlatMapFunction();

        // wrap user defined function into a the corresponding operator
        testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
                new StreamFlatMap<>(statefulFlatMapFunction),
                x -> "a", // dummy key
                Types.STRING);

        // optionally configured the execution environment
        testHarness.getExecutionConfig().setAutoWatermarkInterval(50);

        // open the test harness (will also call open() on RichFunctions)
        testHarness.open();
    }

    @Test
    public void testingStatefulFlatMapFunction() throws Exception {

        //push (timestamped) elements into the operator (and hence user defined function)
        testHarness.processElement("a", 100L);

        //retrieve list of emitted records for assertions
//        assertThat(testHarness.getOutput(), containsInExactlyThisOrder(3L)); // Where is containsInExactlyThisOrder() defined?
        assertEquals(
                Lists.newArrayList(new StreamRecord<>(1L, 100L)),
                this.testHarness.extractOutputStreamRecords()
        );

        testHarness.processElement("b", 120L);
        assertEquals(
                Lists.newArrayList(
                        new StreamRecord<>(1L, 100L),
                        new StreamRecord<>(2L, 120L)),
                this.testHarness.extractOutputStreamRecords()
        );
    }
}
