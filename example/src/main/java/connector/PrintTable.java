package connector;

import common.sources.BoundedRateGenerator;
import common.sources.FixedInputGenerator;
import common.datatypes.Rate;
import common.utils.Utilities;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import table.Input;

import static org.apache.flink.table.api.Expressions.$;

public class PrintTable {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        env.setParallelism(4);

        DataStream<Rate> input = env.addSource(new BoundedRateGenerator());
        tableEnv.executeSql("CREATE TABLE `result` (id INTEGER, `timestamp` BIGINT) WITH ('connector' = 'print', 'print-identifier' = 'hello', 'standard-error' = 'false')");
        Table result = tableEnv.fromDataStream(input, $("id"), $("timestamp"));
        result.executeInsert("result");
    }
}

/*
hello> +I(0,1621671162631)
hello> +I(1,1621671163631)
hello> +I(2,1621671164631)
hello> +I(3,1621671165631)
hello> +I(4,1621671166631)
hello> +I(5,1621671167631)
hello> +I(6,1621671168631)
...
 */