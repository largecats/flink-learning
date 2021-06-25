package connector;

import common.datatypes.Rate;
import common.sources.BoundedRateGenerator;
import common.sources.FixedInputGenerator;
import common.utils.Utilities;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import table.Input;

import static org.apache.flink.table.api.Expressions.$;

public class LocalFileSinkTable {
    public static final String OUTPUT_DIRECTORY = ".\\output\\example\\connector\\LocalFileSinkTable";

    public static void main(String[] args) throws Exception {
        Utilities.removeDir(OUTPUT_DIRECTORY); // clear output directory
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        env.enableCheckpointing(10000); // checkpoint every 10s

        DataStream<Rate> input = env.addSource(new BoundedRateGenerator()).keyBy(x -> (x.id % 10));
        tableEnv.executeSql("CREATE TABLE `result` (event INTEGER, `timestamp` BIGINT) WITH ('connector' = 'filesystem', 'path' = '.\\output\\LocalFileSinkTable', 'format' = 'csv', 'sink.rolling-policy.file-size' = '1024', 'sink.rolling-policy.rollover-interval' = '60s', 'sink.rolling-policy.check-interval' = '10s')");
        tableEnv.createTemporaryView("input", input, $("event"), $("timestamp"));
        tableEnv.executeSql("INSERT INTO `result` SELECT * FROM input");

    }
}
