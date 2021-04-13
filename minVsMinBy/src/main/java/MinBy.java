import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class MinBy {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List data = new ArrayList<Tuple2<Integer, Byte>>();
        data.add(new Tuple2<>(0, 'a'));
        data.add(new Tuple2<>(0, 'b'));
        data.add(new Tuple2<>(0, 'c'));
        data.add(new Tuple2<>(1, 'x'));
        data.add(new Tuple2<>(1, 'y'));
        data.add(new Tuple2<>(1, 'z'));

        DataStreamSource<Tuple2<Integer, Byte>> source = env.fromCollection(data);
        source.keyBy(x -> x.f0).minBy(1).print();

        env.execute();
    }
}
