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

public class ConvertDataStreamAndView {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        DataStream<Tuple5<String, Long, String, Double, Long>> input = env.addSource(new FixedInputGenerator().getGeneratorForTable(Input.orders));
        tableEnv.createTemporaryView("orders", input, $("cCountry"), $("cID"), $("cName"), $("revenue"), $("timestamp")); // Create virtual table

        Table projTable = tableEnv.from("orders").select($("cCountry"));
        tableEnv.toAppendStream(projTable, Row.class).print();

        env.execute();
    }
}

/*
8> FRANCE
1> US
2> FRANCE
3> CHINA
 */