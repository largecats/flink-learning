package common.sources;

import common.datatypes.Rate;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.time.Instant;

public class RateGenerator implements SourceFunction<Rate> {
    public static final int SLEEP_MILLIS_PER_EVENT = 1000;
    public static final Rate START = new Rate(0, Instant.now().toEpochMilli()); // The last accumulate_interval in the first partition may be incomplete
//    public static final  Rate START = Tuple2.of(0, 0L); // The last accumulate_interval in the first partition would be complete

    protected volatile boolean running = true;
    protected volatile Rate element = null;

    @Override
    public void run(SourceContext<Rate> ctx) throws Exception {
        while (running) {
            if (element == null) {
                element = START;
            } else {
                element = new Rate(element.id+1, element.timestamp+1000);
            }
            Thread.sleep(SLEEP_MILLIS_PER_EVENT);
            ctx.collectWithTimestamp(element, element.timestamp);
            ctx.emitWatermark(new Watermark(element.timestamp));
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}