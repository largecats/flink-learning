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
        ParameterTool parameter = ParameterTool.fromArgs(args); // e.g., --topic test --server data-kafka00:8920
        String topic = parameter.get("topic");
        String server = parameter.get("server");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", server);

        FlinkKafkaConsumer myConsumer = new FlinkKafkaConsumer(Arrays.asList(topic), new SimpleStringSchema(), properties);
        myConsumer.setStartFromLatest();

        // TBD
        DataStream<Rate> input = env.addSource(myConsumer).map(
                new MapFunction<String, Rate>() {
                    @Override
                    public Rate map(String value) throws Exception {
                        ObjectMapper mapper = new ObjectMapper();
                        Rate rate = mapper.readValue(value, Rate.class);
                        return rate;
                    }
                }
        );

        DataStream<Rate> output = input;

        env.execute();
    }
}
