package testing;

import org.apache.flink.util.Collector;
import org.junit.Test;
import org.mockito.Mockito;
import static org.mockito.Mockito.*; // or import like this

public class IncrementFlatMapFunctionTest {

    @Test
    public void testIncrement() throws Exception {
        // instantiate your function
        IncrementFlatMapFunction incrementer = new IncrementFlatMapFunction();

        Collector<Long> collector = Mockito.mock(Collector.class);

        // call the methods that you have implemented
        incrementer.flatMap(2L, collector);

        //verify collector was called with the right output
        Mockito.verify(collector, Mockito.times(1)).collect(3L);
    }
}