package testing;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class StatefulFlatMapFunction extends RichFlatMapFunction<String, Long> {
    ValueState<Long> count; // dummy state

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Long> countDescriptor = new ValueStateDescriptor<Long>("count", Types.LONG);
        this.count = getRuntimeContext().getState(countDescriptor);
    }

    @Override
    public void flatMap(String element, Collector<Long> out) throws Exception {
        Long count = 0L;
        if (this.count.value() != null) {
            count = this.count.value();
        }
        count++;
        this.count.update(count);
        out.collect(count);
    }
}