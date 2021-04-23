package operator;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

public class MinVsMinBy {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple3<Integer, Integer, Integer>> source = env.fromElements(
                Tuple3.of(0, 2, 3),
                Tuple3.of(0, 1, 1),
                Tuple3.of(0, 3, 4),
                Tuple3.of(1, 3, 4),
                Tuple3.of(1, 2, 9),
                Tuple3.of(1, 0, 2)
        );
        Iterator<Tuple3<Integer, Integer, Integer>> minResult = source.keyBy(x -> x.f0).min(2).executeAndCollect(); // Key by 1st field, min by 3rd field within each key
        System.out.println("min() output:");
        while (minResult.hasNext()) {
            System.out.println(minResult.next());
        }
        System.out.println("minBy() output:");
        Iterator<Tuple3<Integer, Integer, Integer>> minByResult = source.keyBy(x -> x.f0).minBy(2).executeAndCollect();
        while (minByResult.hasNext()) {
            System.out.println(minByResult.next());
        }
    }
}

/*
min() output:
6> (0,2,3)
6> (0,2,1) // 3rd field correctly updated with the minimum so far, but the first two fields remain unchanged
6> (0,2,1) // No change since the 3rd field is not smaller than current minimum
6> (1,3,4) // First two fields are updated because the key has changed
6> (1,3,4)
6> (1,3,2) // 3rd field updated with the minimum so far, but the first two fields remain unchanged

minBy() output:
6> (0,2,3)
6> (0,1,1) // The entire element is updated if a new minimum is found
6> (0,1,1)
6> (1,3,4)
6> (1,3,4)
6> (1,0,2)
 */
