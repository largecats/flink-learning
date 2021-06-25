package unittest;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class IncrementFlatMapFunction implements FlatMapFunction<Long, Long> {

    @Override
    public void flatMap(Long record, Collector<Long> out) throws Exception {
        out.collect(record + 1);
    }
}