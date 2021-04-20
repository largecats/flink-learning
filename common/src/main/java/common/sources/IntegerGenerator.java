package common.sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import java.util.Random;

public class IntegerGenerator implements SourceFunction<Integer> {
    public static final int SLEEP_MILLIS_PER_EVENT = 1;
    private volatile boolean running = true;


    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        while (running) {
            Random rand = new Random();
            int n = rand.nextInt(10); // Get random int in [0, 10]

            long eventTime = System.currentTimeMillis();
            ctx.collectWithTimestamp(n, eventTime);
            ctx.emitWatermark(new Watermark(eventTime));

            Thread.sleep(IntegerGenerator.SLEEP_MILLIS_PER_EVENT);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
