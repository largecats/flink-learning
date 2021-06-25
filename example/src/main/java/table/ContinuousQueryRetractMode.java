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

public class ContinuousQueryRetractMode {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        DataStream<Tuple3<String, String, Long>> input = env.addSource(new FixedInputGenerator().getGenerator(Input.clicks));
        tableEnv.createTemporaryView("clicks", input, $("user"), $("url"), $("cTime"));

        Table clickCounts = tableEnv.sqlQuery(
                "SELECT user, COUNT(url) as cnt FROM clicks GROUP BY user"
        );
        tableEnv.toRetractStream(clickCounts, Row.class).print();

        env.execute();
    }
}

/*
3> (true,Mary,1)
5> (true,Bob,1)
3> (false,Mary,1) // DELETE
3> (true,Mary,2) // INSERT
3> (false,Mary,2)
5> (false,Bob,1)
3> (true,Mary,3)
5> (true,Bob,2)
5> (true,Liz,1)
3> (false,Mary,3)
5> (false,Liz,1)
3> (true,Mary,4)
5> (true,Liz,2)
5> (false,Liz,2)
5> (true,Liz,3)
5> (false,Bob,2)
5> (true,Bob,3)
5> (false,Bob,3)
5> (true,Bob,4)

 */
