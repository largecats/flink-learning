import org.apache.commons.lang3.concurrent.ConcurrentUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.scala.async.ResultFuture;
import org.apache.flink.streaming.api.scala.async.RichAsyncFunction;

import javax.xml.transform.Result;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class AsyncIO {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stream = env.fromElements(
                "a",
                "b",
                "c",
                "c",
                "d");
        DataStream<Tuple2<String, String>> resultStream = AsyncDataStream.unorderedWait(stream, new AsyncOperation(), 1000, TimeUnit.MILLISECONDS, 100);

        env.execute();
    }

    // Sends requests and sets callback.
    public static class AsyncOperation extends RichAsyncFunction<String, Tuple2<String, String>> {

        private transient StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        private transient Map<String, String> map = new HashMap<>(); // Simulate the database client

        // Query method of the "database client"
        Future<String> query(Map<String, String> map, String key) throws Exception {
            Thread.sleep(5000);
            return ConcurrentUtils.constantFuture(map.get(key));
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            map.put("a", "apple");
            map.put("b", "banana");
            map.put("c", "cat");
            map.put("d", "dog");
        }

        @Override
        public void close() throws Exception {
            map = new HashMap<>();
        }

        @Override
        public void asyncInvoke(String key, final ResultFuture<Tuple2<String, String>> resultFuture) throws Exception {

            // Issue the async query request, receive a Future as result
            final Future<String> result = query(map, key);

            // Set callback to be executed once the request is complete
            CompletableFuture.supplyAsync(new Supplier<String>() {

                // The callback forwards the request result to the result Future
                @Override
                public String get() {
                    try {
                        return result.get();
                    } catch (InterruptedException | ExecutionException e) {
                        return null;
                    }
                }
            }).thenAccept( (String dbResult) -> {
                resultFuture.complete(Collections.singleton(new Tuple2<>(key, dbResult)));
            });
        }
    }
}
