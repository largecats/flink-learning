package testing;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReadTextFile {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> text = env.readTextFile(".\\input\\numbers.txt");

        DataStream<Integer> parsed = text.map(x -> Integer.parseInt(x));

        parsed.print();

        env.execute();
    }
}

/*
Output:
1
2
3
4
5
6
7
8
9
10
 */
