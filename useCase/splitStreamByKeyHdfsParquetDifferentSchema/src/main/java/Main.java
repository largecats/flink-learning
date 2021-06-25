import common.datatypes.EnrichedRate;
import common.sources.BoundedRateGenerator;
import common.utils.Utilities;
import common.datatypes.Rate;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final Integer[] SPECIAL_PARTITIONS = new Integer[]{1, 3, 5, 7, 9};

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args); // e.g., --outputDirectory hdfs://acluster/user/<username>/flink/flink-learning/useCase/splitStreamByKeyHdfsParquetDifferentSchema
        String outputDirectory = parameter.get("outputDirectory");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Streaming part files are only moved to "finished" state upon checkpointing, so must enable checkpointing
        env.enableCheckpointing(60000); // checkpoint every 60s

        DataStream<Rate> input = env.addSource(new BoundedRateGenerator()).keyBy(x -> (x.id % 10)); // Generates 100 records, 1 record per second

        OutputFileConfig config = OutputFileConfig // More convenient to open if all part files have the same suffix
                .builder()
                .withPartPrefix("prefix")
                .withPartSuffix(".ext")
                .build();

        // Schema 1
        FileSink<Rate> sink = FileSink
                .<Rate>forBulkFormat(new Path(outputDirectory), ParquetAvroWriters.forReflectRecord(Rate.class))
                .withBucketAssigner(new KeyBucketAssigner()) // Custom bucket assigner that assigns elements to buckets (subdirectories) based on their key
                .withRollingPolicy(
                        OnCheckpointRollingPolicy.build()
                )
                .withOutputFileConfig(config)
                .build();

        // Schema 2
        FileSink<EnrichedRate> specialSink = FileSink
                .<EnrichedRate>forBulkFormat(new Path(outputDirectory), ParquetAvroWriters.forReflectRecord(EnrichedRate.class))
                .withBucketAssigner(new SpecialKeyBucketAssigner()) // Custom bucket assigner that assigns elements to buckets (subdirectories) based on their key
                .withRollingPolicy(
                        OnCheckpointRollingPolicy.build()
                )
                .withOutputFileConfig(config)
                .build();

        input.filter(x -> Arrays.asList(SPECIAL_PARTITIONS).contains(x.id % 10)).map(x -> new EnrichedRate(x)).returns(EnrichedRate.class).sinkTo(specialSink);
        input.filter(x -> !Arrays.asList(SPECIAL_PARTITIONS).contains(x.id % 10)).sinkTo(sink);


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

    public static class SpecialKeyBucketAssigner implements BucketAssigner<EnrichedRate, String> {

        @Override
        public String getBucketId(EnrichedRate element, Context context) {
            System.out.println("element.id = " + element.id + ", element.timestamp = " + element.timestamp + ", element.flag = " + element.flag);
            return String.valueOf(element.id % 10);
        }

        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }
    }
}
