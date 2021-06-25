import common.sources.BoundedRateGenerator;
import common.utils.Utilities;
import common.datatypes.Rate;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class DifferentSchema {
    private static final String OUTPUT_DIRECTORY = ".\\output\\useCase\\splitStreamByKey\\DifferentSchema";
    private static final Integer[] SPECIAL_PARTITIONS = new Integer[]{1, 3, 5, 7, 9};

    public static void main(String[] args) throws Exception {
        Utilities.removeDir(OUTPUT_DIRECTORY); // clear output directory
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Streaming part files are only moved to "finished" state upon checkpointing, so must enable checkpointing
        env.enableCheckpointing(10000); // checkpoint every 10s

        DataStream<Rate> input = env.addSource(new BoundedRateGenerator()).keyBy(x -> (x.id % 10)); // Generates 100 records, 1 record per second

        OutputFileConfig config = OutputFileConfig // More convenient to open if all part files have the same suffix
                .builder()
                .withPartPrefix("prefix")
                .withPartSuffix(".ext")
                .build();

        // Schema 1
        FileSink<Rate> sink = FileSink
                .<Rate>forRowFormat(new Path(OUTPUT_DIRECTORY), new SimpleStringEncoder<>())
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

        // Schema 2
        FileSink<Integer> specialSink = FileSink
                .<Integer>forRowFormat(new Path(OUTPUT_DIRECTORY), new SimpleStringEncoder<>())
                .withBucketAssigner(new SpecialKeyBucketAssigner()) // Custom bucket assigner that assigns elements to buckets (subdirectories) based on their key
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.SECONDS.toMillis(60))
                                .withInactivityInterval(TimeUnit.SECONDS.toMillis(1))
                                .withMaxPartSize(1024)
                                .build()
                )
                .withOutputFileConfig(config)
                .build();

        input.filter(x -> Arrays.asList(SPECIAL_PARTITIONS).contains(x.id % 10)).map(x ->x.id).returns(Types.INT).sinkTo(specialSink);
        input.filter(x -> !Arrays.asList(SPECIAL_PARTITIONS).contains(x.id % 10)).sinkTo(sink);


        env.execute();
    }

    /*
    Assigns elements to buckets based on their key.
     */
    public static class KeyBucketAssigner implements BucketAssigner<Rate, String> {

        @Override
        public String getBucketId(Rate element, Context context) {
            return String.valueOf(element.id % 10);
        }

        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }
    }

    public static class SpecialKeyBucketAssigner implements BucketAssigner<Integer, String> {

        @Override
        public String getBucketId(Integer element, Context context) {
            return String.valueOf(element % 10);
        }

        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }
    }
}
