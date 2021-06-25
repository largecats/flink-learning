package operator;

import common.sources.FixedInputGenerator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class Project {
    public static Tuple4<String, String, String, String>[] data = new Tuple4[]{
            Tuple4.of("a", "apple", "13", "Australia"),
            Tuple4.of("b", "banana", "5", "India")
    };

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple4<String, String, String, String>> input = env.fromElements(data);

        input.project(0, 1).distinct(0).print();

        // For batch processing based on DataSet, don't need env.execute() as print() already triggers execution
        // env.execute() here will trigger "No new data sinks have been defined since the last execution" error
    }
}
