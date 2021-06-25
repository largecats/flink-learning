package connector;

import common.sources.BoundedRateGenerator;
import common.utils.Utilities;
import common.datatypes.Rate;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class LocalFileSink {

    public static final String OUTPUT_DIRECTORY = ".\\output\\example\\connector\\LocalFileSink";

    public static void main(String[] args) throws Exception {
        Utilities.removeDir(OUTPUT_DIRECTORY); // clear output directory
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Streaming part files are only moved to "finished" state upon checkpointing, so must enable checkpointing
        env.enableCheckpointing(10000); // checkpoint every 10s

        // Can use DataStream<Rate> instead of KeyedStream<Rate, Integer> here because KeyedStream is a subtype of DataStream; but this is not true for other classes like WindowedStream
        DataStream<Rate> input = env.addSource(new BoundedRateGenerator()).keyBy(x -> (x.id % 10)); // Generates 100 records, 1 record per second

        OutputFileConfig config = OutputFileConfig // More convenient to open if all part files have the same suffix
                .builder()
                .withPartPrefix("prefix")
                .withPartSuffix(".ext")
                .build();

        FileSink<Rate> sink = FileSink
                .<Rate>forRowFormat(new Path(OUTPUT_DIRECTORY), new SimpleStringEncoder<>("UTF-8"))
//                .withBucketCheckInterval(60000) // How often a bucket is checked for rolling
                .withRollingPolicy(
                        DefaultRollingPolicy.builder() // Rolls part files if:
                        .withRolloverInterval(TimeUnit.SECONDS.toMillis(60)) // Part file has >= 60s worth of data, or
                        .withInactivityInterval(TimeUnit.SECONDS.toMillis(1)) // Part file has not received new records for 5s, or
                        .withMaxPartSize(1024) // Part file size exceeds 1KB
                        .build()
                )
                .withOutputFileConfig(config)
                .build();

        input.sinkTo(sink);

        env.execute();
    }
}
