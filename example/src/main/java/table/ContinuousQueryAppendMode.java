package table;

import common.sources.FixedInputGenerator;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import java.time.LocalDateTime;

import static org.apache.flink.table.api.Expressions.$;

public class ContinuousQueryAppendMode {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        DataStream<Tuple3<String, String, Long>> input = env.addSource(new FixedInputGenerator().getGenerator(Input.clicks)); // the timestamp column needs to be in milliseconds!
        tableEnv.createTemporaryView("clicks", input, $("user"), $("url"), $("cTime").rowtime()); // Must convert to Timestamp, otherwise will be recognized as BIGINT, see https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/streaming/time_attributes.html#during-datastream-to-table-conversion

        Table clickCounts = tableEnv.sqlQuery(
                "SELECT user, TUMBLE_END(cTime, INTERVAL '1' HOURS) AS endT, COUNT(url) AS cnt " +
                        "FROM clicks " +
                        "GROUP BY user, TUMBLE(cTime, INTERVAL '1' HOURS)"
        );
        tableEnv.toRetractStream(clickCounts, Row.class).print();

        env.execute();
    }
}

/*
5> (true,Bob,2021-01-01T05:00,1)
3> (true,Mary,2021-01-01T05:00,3)
3> (true,Mary,2021-01-01T07:00,1)
5> (true,Bob,2021-01-01T06:00,1)
5> (true,Liz,2021-01-01T06:00,2)
5> (true,Liz,2021-01-01T07:00,1)
5> (true,Bob,2021-01-01T07:00,2)
 */
