package common.sources;

import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

public class FixedInputGenerator {

    public static final int SLEEP_MILLIS_PER_EVENT = 10;

    public SourceFunction<Tuple3<String, String, Long>> getGenerator(Tuple3<String, String, Long>[] input) {
        class Generator implements SourceFunction<Tuple3<String, String, Long>> {
            private volatile boolean running = true;

            @Override
            public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws Exception {
                int i = 0;
                while (running && i < input.length) {
                    Tuple3<String, String, Long> element = input[i]; // Array iterator is not serializable, so need to
                    // use array
                    i++;

                    Thread.sleep(SLEEP_MILLIS_PER_EVENT);

                    ctx.collectWithTimestamp(element, element.f2);
                    ctx.emitWatermark(new Watermark(element.f2));
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        }
        return new Generator();
    }

    public SourceFunction<Tuple5<String, Long, String, Double, Long>> getGeneratorForTable(Tuple5<String, Long, String, Double, Long>[] input) {
        class Generator implements SourceFunction<Tuple5<String, Long, String, Double, Long>> {
            private volatile boolean running = true;

            @Override
            public void run(SourceContext<Tuple5<String, Long, String, Double, Long>> ctx) throws Exception {
                int i = 0;
                while (running && i < input.length) {
                    Tuple5<String, Long, String, Double, Long> element = input[i]; // Array iterator is not serializable, so need to
                    // use array
                    i++;

                    Thread.sleep(SLEEP_MILLIS_PER_EVENT);

                    ctx.collectWithTimestamp(element, element.f4);
                    ctx.emitWatermark(new Watermark(element.f4));
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        }
        return new Generator();
    }
}


