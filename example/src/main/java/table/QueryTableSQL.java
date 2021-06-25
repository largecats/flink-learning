package table;

import common.sources.FixedInputGenerator;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import static org.apache.flink.table.api.Expressions.$;

public class QueryTableSQL {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        DataStream<Tuple5<String, Long, String, Double, Long>> input = env.addSource(new FixedInputGenerator().getGeneratorForTable(Input.orders));
        Table orders = tableEnv.fromDataStream(input, $("cCountry"), $("cID"), $("cName"), $("revenue"));
        tableEnv.createTemporaryView("orders", orders);

        Table revenue = tableEnv.sqlQuery( // different from .executeSql()
                "SELECT cCountry, SUM(revenue) AS revSum " +
                "FROM orders " +
                "GROUP BY cCountry"
        );

        tableEnv.toRetractStream(revenue, TypeInformation.of(new TypeHint<Tuple2<String, Double>>(){})).print();

        env.execute();
    }
}
