package processFunction;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

public class FixedInputGenerator implements SourceFunction<Tuple3<String, String, Long>> {
    private volatile boolean running = true;
    private Tuple3<String, String, Long>[] data = new Tuple3[] {
            Tuple3.of("a", "abase", 0L),
            Tuple3.of("b", "bard", 500L),
            Tuple3.of("a", "abate", 2500L),
            Tuple3.of("b", "barrage", 3000L),
            Tuple3.of("a", "abbreviate", 5000L),
            Tuple3.of("b", "baroque", 5500L),
            Tuple3.of("a", "abdicate", 7500L),
            Tuple3.of("b", "barren", 8000L)
    }; // Array iterator is not serializable

    @Override
    public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws Exception {
        int i = 0;
        Tuple3<String, String, Long> prev = Tuple3.of("", "", 0L);
        while (running && i < data.length) {
            Tuple3<String, String, Long> element = data[i];
            i++;

            System.out.println("Sleeping for " + (element.f2 - prev.f2));
            Thread.sleep(element.f2 - prev.f2);
            prev = element;

            ctx.collectWithTimestamp(element, element.f2);
            ctx.emitWatermark(new Watermark(element.f2));
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
