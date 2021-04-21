//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.datastream.AsyncDataStream;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.scala.async.ResultFuture;
//import org.apache.flink.streaming.api.scala.async.RichAsyncFunction;
//
//import javax.xml.transform.Result;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.Future;
//import java.util.concurrent.TimeUnit;
//import java.util.stream.Stream;
//
//public class AsyncIO {
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        DataStream<String> stream = env.fromElements(
//                "a",
//                "b",
//                "c",
//                "c",
//                "d");
//        DataStream<Tuple2<String, String>> resultStream = AsyncDataStream.unorderedWait(stream, new AsyncOperation(), 1000, TimeUnit.MILLISECONDS, 100);
//
//        env.execute();
//    }
//
//    class AsyncOperation extends RichAsyncFunction<String, Tuple2<String, String>> {
//
//        private transient StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        private transient Map<String, String> map = new HashMap<>();
//
//        @Override
//        public void open(Configuration parameters) throws Exception {
//            map.put("a", "apple");
//            map.put("b", "banana");
//            map.put("c", "cat");
//            map.put("d", "dog");
//        }
//
//        @Override
//        public void close() throws Exception {}
//
//        @Override
//        public void asyncInvoke(String key, final ResultFuture<Tuple2<String, String>> resultFuture) throws Exception {
//
//            final Future<String> result = otherStream.filter(
//                    new FilterFunction<Tuple2<String, String>>() {
//                        @Override
//                        public boolean filter(Tuple2<String, String> element) throws Exception {
//                            return element.f0 == key;
//                        }
//                    }
//            ).;
//        }
//    }
//}
