package table;

import common.sources.FixedInputGenerator;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

public class DropView {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);

        DataStream<Tuple5<String, Long, String, Double, Long>> input = env.addSource(new FixedInputGenerator().getGeneratorForTable(Input.orders));
        tableEnv.createTemporaryView("orders", input, $("cCountry"), $("cID"), $("cName"), $("revenue"), $("timestamp")); // Create virtual table

        System.out.println(Arrays.toString(tableEnv.listViews()));
        tableEnv.executeSql("DROP TEMPORARY VIEW orders"); // If don't have TEMPORARY keyword, will raise Exception in thread "main" org.apache.flink.table.api.ValidationException: Temporary view with identifier '`default_catalog`.`default_database`.`orders`' exists. Drop it first before removing the permanent view.
        System.out.println(Arrays.toString(tableEnv.listViews()));
    }
}

/*
[orders]
[]
 */
