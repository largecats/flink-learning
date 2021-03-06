package common.sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import common.datatypes.Item;

public class ItemGenerator implements SourceFunction<Item> {
    public static final int SLEEP_MILLIS_PER_EVENT = 10;
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Item> ctx) throws Exception {
        while (running) {
            Item item = new Item();

            long eventTime = item.getEventTime();
            ctx.collectWithTimestamp(item, eventTime);
            ctx.emitWatermark(new Watermark(eventTime));

            Thread.sleep(ItemGenerator.SLEEP_MILLIS_PER_EVENT);
        }
    }

    @Override
    public void cancel() { running = false; }

}
