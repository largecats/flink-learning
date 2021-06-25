package table;

import common.sources.FixedInputGenerator;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

public class DescribeTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);

        DataStream<Tuple5<String, Long, String, Double, Long>> input = env.addSource(new FixedInputGenerator().getGeneratorForTable(Input.orders));
        Table orders = tableEnv.fromDataStream(input, $("cCountry"), $("cID"), $("cName"), $("revenue"), $("timestamp").rowtime());
        tableEnv.createTemporaryView("orders", orders); // Create virtual table

        tableEnv.executeSql("DESCRIBE orders").print();
        orders.printSchema();
    }
}

/*
+-----------+------------------------+------+-----+--------+-----------+
|      name |                   type | null | key | extras | watermark |
+-----------+------------------------+------+-----+--------+-----------+
|  cCountry |                 STRING | true |     |        |           |
|       cID |                 BIGINT | true |     |        |           |
|     cName |                 STRING | true |     |        |           |
|   revenue |                 DOUBLE | true |     |        |           |
| timestamp | TIMESTAMP(3) *ROWTIME* | true |     |        |           |
+-----------+------------------------+------+-----+--------+-----------+
5 rows in set
root
 |-- cCountry: STRING
 |-- cID: BIGINT
 |-- cName: STRING
 |-- revenue: DOUBLE
 |-- timestamp: TIMESTAMP(3) *ROWTIME*
 */