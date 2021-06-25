package table;

import common.sources.FixedInputGenerator;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class ConvertDataStreamAndTable {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // Create table from DataStream
        DataStream<Tuple5<String, Long, String, Double, Long>> input = env.addSource(new FixedInputGenerator().getGeneratorForTable(Input.orders));
        Table orders = tableEnv.fromDataStream(input, $("cCountry"), $("cID"), $("cName"), $("revenue"), $("timestamp"));
        Table orders1 = orders.select($("*"));
        orders1.printSchema();
        tableEnv.toAppendStream(orders1, Row.class).print();
        env.execute();
    }
}

/*
root
 |-- cCountry: STRING
 |-- cID: BIGINT
 |-- cName: STRING
 |-- revenue: DOUBLE
 |-- timestamp: BIGINT

1> CHINA,101,xiaoming,1000.0,6
8> FRANCE,401,frank,190.0,5
7> US,502,bob,50.0,3
6> FRANCE,10001,amy,1000.0,0
 */