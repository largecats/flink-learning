package operator;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;

public class CombineGroup {
    public static String[] data = {
            "to", "be", "or", "not", "to", "be"
    };

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> input = env.fromElements(data);

        DataSet<Tuple2<String, Integer>> combinedWords = input
                .groupBy(x -> x) // group identical words, can't use index since DataStream type is not tuple
                .combineGroup(new GroupCombineFunction<String, Tuple2<String, Integer>>() {

                    public void combine(Iterable<String> words, Collector<Tuple2<String, Integer>> out) { // combine
                        String key = null;
                        int count = 0;

                        for (String word : words) {
                            key = word;
                            count++;
                        }
                        // emit tuple with word and count
                        out.collect(new Tuple2(key, count));
                    }
                });
        combinedWords.print();

        DataSet<Tuple2<String, Integer>> output = combinedWords
                .groupBy(0)                              // group by words again
                .reduceGroup(new GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() { // group reduce with full data exchange

                    public void reduce(Iterable<Tuple2<String, Integer>> words, Collector<Tuple2<String, Integer>> out) {
                        String key = null;
                        int count = 0;

                        for (Tuple2<String, Integer> word : words) {
                            key = word.f0;
                            count++;
                        }
                        // emit tuple with word and count
                        out.collect(new Tuple2(key, count));
                    }
                });
        output.print();
    }
}

/*
(be,2)
(not,1)
(or,1)
(to,2)
(or,1)
(not,1)
(to,1)
(be,1)
 */