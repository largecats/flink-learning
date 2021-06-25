import com.google.common.collect.Iterables;
import common.datatypes.Rate;
import common.datatypes.RateSchema;
import common.sources.BoundedRateGenerator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.runtime.operators.window.CountWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.IterableUtils;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.stream.Stream;

public class Main {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<Rate> input = env.addSource(new BoundedRateGenerator());

        input.keyBy(x -> x.id % 10).print();

        env.execute();
    }
}

/*
View YARN's full log:
Run below in server, replace hadoop-slave190:8042, container id, and log file type as appropriate (.out for stdout, stderr, and .log for internal logging).
E.g., in server, run
    curl http://hadoop-slave162:8042/node/containerlogs/container_e53_1621591447361_87870_01_000003/log/taskmanager.out/?start=0
to view the output of counts.print().
Can run
    curl http://hadoop-slave162:8042/node/containerlogs/container_e53_1621591447361_87870_01_000003/log/
to see what other logs are available.
Note that /log/taskmanager.out, /out/taskmanager/out, /err/taskamanger.out all point to the same file.
 */