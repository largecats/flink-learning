package testing;

import org.junit.Test;
import static org.junit.Assert.*; // has to be static
import java.util.Optional;

public class IncrementMapFunctionTest {

    @Test
    public void testIncrement() throws Exception {
        // instantiate your function
        IncrementMapFunction incrementer = new IncrementMapFunction();

        // call the methods that you have implemented
        assertEquals(3L, (long) incrementer.map(2L));
    }
}