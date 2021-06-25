import common.sources.BoundedRateGenerator;
import common.utils.Utilities;
import common.datatypes.Rate;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
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
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import java.util.concurrent.TimeUnit;

/**
 * Write stream as parquet files to HDFS.
 */

public class Main {

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args); // e.g., --outputDirectory hdfs://acluster/user/<username>/flink/flink-learning/useCase/splitStreamByKeyHdfsParquetSameSchema
        String outputDirectory = parameter.get("outputDirectory");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Streaming part files are only moved to "finished" state upon checkpointing, so must enable checkpointing
        env.enableCheckpointing(60000); // checkpoint every 60s

        DataStream<Rate> input = env.addSource(new BoundedRateGenerator()).keyBy(x -> (x.id % 10)); // Generates 100 records, 1 record per second
//        input.print();

        FileSink<Rate> sink = FileSink
                .<Rate>forBulkFormat(new Path(outputDirectory), ParquetAvroWriters.forReflectRecord(Rate.class))
                .withBucketAssigner(new KeyBucketAssigner()) // Custom bucket assigner that assigns elements to buckets (subdirectories) based on their key
                .withRollingPolicy(
                        OnCheckpointRollingPolicy.build() // If checkpoint interval is 10s, will have one file for each record due to the modulo 10
                )
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

/*
Output:
/user/<username>/flink/flink-learning/useCase/splitStreamByKeyHdfsParquetSameSchema/0/.part-00801f3f-1184-4f13-9558-c5c8e953f480-1.inprogress.62b4e0fa-ba64-411d-a45d-a4b3d77543c4
/user/<username>/flink/flink-learning/useCase/splitStreamByKeyHdfsParquetSameSchema/0/part-00801f3f-1184-4f13-9558-c5c8e953f480-0
/user/<username>/flink/flink-learning/useCase/splitStreamByKeyHdfsParquetSameSchema/1/.part-be563df0-0bcd-4b5b-af82-6ce6b8ee6498-1.inprogress.5b6a5cf4-4b81-410f-af48-7d3ea3f779e3
/user/<username>/flink/flink-learning/useCase/splitStreamByKeyHdfsParquetSameSchema/1/part-be563df0-0bcd-4b5b-af82-6ce6b8ee6498-0
...
/user/<username>/flink/flink-learning/useCase/splitStreamByKeyHdfsParquetSameSchema/9/.part-1f3a1d0b-1964-4a76-8915-b854da20560d-1.inprogress.d3630c2c-6613-4a13-83d4-9e45be22aaf1
/user/<username>/flink/flink-learning/useCase/splitStreamByKeyHdfsParquetSameSchema/9/part-1f3a1d0b-1964-4a76-8915-b854da20560d-0

Note:
1. If the output directory only contains the first record, there may be error in writing the first record, and Flink keeps retrying from where it
failed last time. check taskmanager.log to look for errors. E.g.,:
curl http://hadoop-slave190:8042/node/containerlogs/container_e53_1621591447361_37620_01_000001/log/taskmanager.log/?start=0
2. Can read output using spark via:
    val data = spark.read.parquet("/user/<username>/flink/flink-learning/useCase/splitStreamByKeyHdfsParquetSameSchema/0")
    This will not include the data in the inprogress files (as designed, since inprogress files are not safe to read). If need to look at inprogress files,
    rename it to remove the . in front, e.g., /.part-xxx to /part-xxx.
 */