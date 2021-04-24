package testing;

import org.apache.flink.api.common.functions.MapFunction;

public class IncrementMapFunction implements MapFunction<Long, Long> {

    @Override
    public Long map(Long record) throws Exception {
        return record + 1;
    }
}