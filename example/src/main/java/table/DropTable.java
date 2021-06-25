package table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;

public class DropTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);

        // Create connector table
        tableEnv.executeSql("CREATE TABLE Orders (`user_id` BIGINT, `price` DOUBLE, `quantity` INTEGER, `cost` AS price * quantity) WITH ('connector' = 'print')"); // See https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/connectors/formats/csv.html
        tableEnv.executeSql(
                "INSERT INTO Orders VALUES " +
                        "(1001, 100.0, 2)," +
                        "(1003, 50.0, 10)"
        );
        System.out.println(Arrays.toString(tableEnv.listTables()));
        tableEnv.executeSql("DROP TABLE Orders");
        System.out.println(Arrays.toString(tableEnv.listTables()));
    }
}

/*
[Orders]
[]
+I(1001,100.0,2)
+I(1003,50.0,10)
 */