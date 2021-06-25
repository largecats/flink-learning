package table.window;

import common.datatypes.Rate;
import common.datatypes.RateCount;
import common.sources.BoundedRateGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

import java.time.Duration;

public class TumblingProcessingTimeWindowTableAPI {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);

        // Register output table sink
        tableEnv.executeSql("CREATE TABLE output (id INTEGER, `count` BIGINT) WITH ('connector' = 'print')");

        DataStream<Rate> stream = env.addSource(new BoundedRateGenerator())
                .map(event -> new Rate(event.id % 2, event.timestamp))
                .keyBy(event -> event.id);

        Table table = tableEnv.fromDataStream(stream, $("id"), $("timestamp"), $("processing_time").proctime());

        Table output = table.window(Tumble
                .over(lit(10).seconds())
                .on($("processing_time"))
                .as("TumblingWindow"))
                .groupBy($("TumblingWindow"), $("id"))
                .select($("id"), $("timestamp").count().as("count"));

        output.executeInsert("output");

    }
}

/*
...
+I[1, 5]
+I[0, 5]
+I[1, 5]
+I[0, 5]
...
 */