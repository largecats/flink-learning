package operator;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Iterator;

public class MapAndReduce {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> input = env.fromElements(
                "to", "be", "or", "not", "to", "be"
        );

        input
                .map(x -> x.length())
                .keyBy(x -> "a") // dummy key
                .reduce((x, y) -> x+y)
                .print();

        env.execute();
    }
}

/*
6> 2
6> 4
6> 6
6> 9
6> 11
6> 13
 */