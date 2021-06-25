package table;

import common.sources.FixedInputGenerator;
import common.utils.Utilities;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class AddColumns {

    public static final String OUTPUT_DIRECTORY = ".\\output\\example\\table\\AddColumns";

    public static void main(String[] args) throws Exception {
        Utilities.removeDir(OUTPUT_DIRECTORY); // Clear output directory

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);

        DataStream<Tuple5<String, Long, String, Double, Long>> input = env.addSource(new FixedInputGenerator().getGeneratorForTable(Input.orders));
        Table orders = tableEnv.fromDataStream(input, $("region"), $("user_id"), $("username"), $("price"), $("quantity"));
        tableEnv.createTemporaryView("orders", orders);

        tableEnv.executeSql("CREATE TABLE `result` (user_id BIGINT, price DOUBLE, quantity BIGINT, cost DOUBLE) WITH ('connector' = 'filesystem', 'path' = '.\\output\\AddedColumn', 'format' = 'csv', 'csv.ignore-parse-errors' = 'true')"); // See https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/connectors/formats/csv.html
//        Table result = tableEnv.sqlQuery("SELECT user_id, price, quantity, price * quantity AS cost FROM orders"); // Compute cost column form price and quantity
        Table result = orders.addColumns($("quantity").times($("price")).as("cost")).select($("user_id"), $("price"), $("quantity"), $("cost"));
        result.executeInsert("result");
    }
}
