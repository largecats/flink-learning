package table;

import common.utils.Utilities;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class ComputedColumn {
    public static final String OUTPUT_DIRECTORY = ".\\output\\example\\table\\ComputedColumn";

    public static void main(String[] args) throws Exception {
        Utilities.removeDir(OUTPUT_DIRECTORY); // Clear output directory

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);

        // Create connector table
        tableEnv.executeSql("CREATE TABLE Orders (`user_id` BIGINT, `price` DOUBLE, `quantity` INTEGER, `cost` AS price * quantity) WITH ('connector' = 'filesystem', 'path' = 'output\\ComputedColumn', 'format' = 'csv')"); // See https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/connectors/formats/csv.html
        tableEnv.executeSql(
                "INSERT INTO Orders VALUES " +
                        "(1001, 100.0, 2)," +
                        "(1003, 50.0, 10)"
        );
        Table projTable = tableEnv.sqlQuery("SELECT * FROM Orders");
        tableEnv.toAppendStream(projTable, Row.class).print();

        env.execute();
    }
}

/*
1> 1001,100.0,2,200.0
6> 1003,50.0,10,500.0

Before rerun, need to remove old output files, otherwise all previous outputs will be read via SELECT * FROM Orders, and the data will be duplicated.
 */