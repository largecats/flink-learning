import common.sources.BoundedRateGenerator;
import common.utils.Utilities;
import common.datatypes.Rate;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Write stream as text files to HDFS.
 */

public class Main {

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args); // e.g., --outputDirectory hdfs://acluster/user/<username>/flink/flink-learning/useCase/splitStreamByKeyHdfsSameSchema --topic test --server data-kafka00:8920
//        String server = parameter.get("server");
//        String topic = parameter.get("topic");
        String outputDirectory = parameter.get("outputDirectory");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Streaming part files are only moved to "finished" state upon checkpointing, so must enable checkpointing
        env.enableCheckpointing(10000); // checkpoint every 10s
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", server);

        DataStream<Rate> input = env.addSource(new BoundedRateGenerator()).keyBy(x -> (x.id % 10)); // Generates 100 records, 1 record per second
//        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer(Arrays.asList(topic), new SimpleStringSchema(), properties);
//        myConsumer.setStartFromLatest();
//        DataStream<Rate> input = env.addSource(myConsumer).map(
//                new MapFunction<String, Rate>() {
//                    @Override
//                    public Rate map(String value) throws Exception {
//                        ObjectMapper mapper = new ObjectMapper();
//                        Rate rate = mapper.readValue(value, Rate.class);
//                        return rate;
//                    }
//                }
//        );

//        input.print();

        OutputFileConfig config = OutputFileConfig // More convenient to open if all part files have the same suffix
                .builder()
                .withPartPrefix("prefix")
                .withPartSuffix(".ext")
                .build();

        FileSink<Rate> sink = FileSink
                .<Rate>forRowFormat(new Path(outputDirectory), new SimpleStringEncoder<>("UTF-8"))
                .withBucketAssigner(new KeyBucketAssigner()) // Custom bucket assigner that assigns elements to buckets (subdirectories) based on their key
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.SECONDS.toMillis(60))
                                .withInactivityInterval(TimeUnit.SECONDS.toMillis(1))
                                .withMaxPartSize(1024)
                                .build()
                )
                .withOutputFileConfig(config)
                .build();

        input.sinkTo(sink);

        env.execute();
    }

    /*
    Assigns elements to buckets based on their key.
     */
    public static class KeyBucketAssigner implements BucketAssigner<Rate, String> {

        @Override
        public String getBucketId(Rate element, Context context) {
            System.out.println("element.id = " + element.id + ", element.timestamp = " + element.timestamp);
            return String.valueOf(element.id % 10);
        }

        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }
    }
}
