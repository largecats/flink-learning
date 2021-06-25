package table;

import common.sources.FixedInputGenerator;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class EmitTable {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // Register output table sink
        tableEnv.executeSql("CREATE TABLE Orders (cCountry STRING, cID BIGINT, cName STRING, revenue DOUBLE, `timestamp` BIGINT) WITH ('connector' = 'print')");

        // Create table from DataStream
        DataStream<Tuple5<String, Long, String, Double, Long>> input = env.addSource(new FixedInputGenerator().getGeneratorForTable(Input.orders));
        Table orders = tableEnv.fromDataStream(input, $("cCountry"), $("cID"), $("cName"), $("revenue"), $("timestamp"));
        orders.executeInsert("Orders"); // Emit table to Print sink
    }
}

/*
+I(FRANCE,10001,amy,1000.0,0)
+I(US,502,bob,50.0,3)
+I(FRANCE,401,frank,190.0,5)
+I(CHINA,101,xiaoming,1000.0,6)
 */