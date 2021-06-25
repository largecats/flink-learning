package async;

import common.datatypes.TaxiRide;
import common.sources.TaxiRideGenerator;
import org.apache.commons.lang3.concurrent.ConcurrentUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class AsyncIOStreaming {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stream = env.addSource(new TaxiRideGenerator())
                .map(x -> x.rideId)
                .filter(x -> x%11 == 0) // generator is too fast, slow down input
                .map(x -> (x % 5)) // the map's keys range from 1 to 4
                .map(x -> x.toString());
        DataStream<Tuple2<String, String>> resultStream = AsyncDataStream.unorderedWait( // Result records are emitted
                // as soon as the async request finishes, order of stream elements can be different from before
                stream,
                new AsyncOperation(),
                10000, // How long an async request will take before it's considered failed; decrease to 1000 to see
                // timeout
                TimeUnit.MILLISECONDS,
                100); // How many async requests can happen at the same time

        resultStream.print();

        env.execute();
    }

    // Sends requests and sets callback.
    public static class AsyncOperation extends RichAsyncFunction<String, Tuple2<String, String>> {

        private transient StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        private transient Map<String, String> map;

        // Query method of the "database client"
        Future<String> query(Map<String, String> map, String key) throws Exception {
            Thread.sleep(5000);
            return ConcurrentUtils.constantFuture(map.get(key));
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            map = new HashMap<>(); // Simulate the database client
            map.put("1", "apple");
            map.put("2", "banana");
            map.put("3", "cat");
            map.put("4", "dog");
        }

        @Override
        public void close() throws Exception {
            map = new HashMap<>();
        }

        @Override
        public void asyncInvoke(String input, final ResultFuture<Tuple2<String, String>> resultFuture) throws Exception {

            // Issue the async query request, receive a Future as result
            final Future<String> result = query(map, input);

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
                resultFuture.complete(Collections.singleton(new Tuple2<>(input, dbResult))); // Completed with first
                // call, subsequent calls are ignored
            });
        }
    }
}

/*
Something like
5> (1,apple)
6> (2,banana)
1> (3,cat)
3> (4,dog)
2> (4,dog)
8> (3,cat)
4> (0,null)
7> (2,banana)
5> (2,banana)
6> (0,null)
1> (1,apple)
3> (0,null)
2> (2,banana)
8> (4,dog)
4> (1,apple)
 */