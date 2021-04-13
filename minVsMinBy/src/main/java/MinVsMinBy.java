import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class MinVsMinBy {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List data = new ArrayList<Tuple3<Integer, Integer, Integer>>();
        data.add(new Tuple3<>(0, 2, 3));
        data.add(new Tuple3<>(0, 1, 1));
        data.add(new Tuple3<>(0, 3, 4));
        data.add(new Tuple3<>(1, 3, 4));
        data.add(new Tuple3<>(1, 2, 9));
        data.add(new Tuple3<>(1, 0, 2));

        DataStreamSource<Tuple3<Integer, Integer, Integer>> source = env.fromCollection(data);
        source.keyBy(x -> x.f0).min(2).print(); // Key by 1st field, min by 3rd field within each key
//        source.keyBy(x -> x.f0).minBy(2).print();

        env.execute();
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
6> (0,1,1)
6> (0,1,1)
6> (1,3,4)
6> (1,3,4)
6> (1,0,2)
 */
