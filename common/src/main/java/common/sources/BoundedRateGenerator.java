package common.sources;

import common.datatypes.Rate;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.time.Instant;

public class BoundedRateGenerator extends RateGenerator {
    public int MAX_COUNT;
    public BoundedRateGenerator() {
        MAX_COUNT = 100;
    }
    public BoundedRateGenerator(int size) {
        MAX_COUNT = size;
    }
    @Override
    public void run(SourceContext<Rate> ctx) throws Exception {
        while (running & MAX_COUNT > 0) {
            if (element == null) {
                element = START;
            } else {
                element = new Rate(element.id+1, element.timestamp+1000);
            }
            Thread.sleep(SLEEP_MILLIS_PER_EVENT);
            ctx.collectWithTimestamp(element, element.timestamp);
            ctx.emitWatermark(new Watermark(element.timestamp));
            MAX_COUNT --;
        }
    }
}