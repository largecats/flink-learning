package table;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class CreateConnectorTable {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // Create connector table
        // tableEnv.connect() is deprecated, add connector using tableEnv.executeSql() instead.
        tableEnv.executeSql("CREATE TABLE Orders (cCountry STRING, cID BIGINT, cName STRING, revenue DOUBLE, `timestamp` BIGINT) WITH ('connector' = 'print')"); // See https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/connectors/print.html
        tableEnv.executeSql(
                "INSERT INTO Orders VALUES " +
                        "('FRANCE', 10001, 'amy', 1000.0, 0)," +
                        "('US', 502, 'bob', 50.0, 3)," +
                        "('FRANCE', 401, 'frank', 190.0, 5)," +
                        "('CHINA', 101, 'xiaoming', 1000.0, 6)"
        );
//        Table orders = tableEnv.from("Orders");
//        tableEnv.toAppendStream(orders, Row.class).print(); // error: Connector 'print' can only be used as a sink. It cannot be used as a source.
    }
}

/*
root
 |-- cCountry: STRING
 |-- cID: BIGINT
 |-- cName: STRING
 |-- revenue: DOUBLE
 |-- timestamp: BIGINT

+I(FRANCE,10001,amy,1000.0,0)
+I(US,502,bob,50.0,3)
+I(FRANCE,401,frank,190.0,5)
+I(CHINA,101,xiaoming,1000.0,6)
 */