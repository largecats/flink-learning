package table.udf;

import common.sources.FixedInputGenerator;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class AggregateFunctionUdf {
    // mutable accumulator of structured type for the aggregate function
    public static class WeightedAvgAccumulator {
        public double sum = 0;
        public long count = 0;
    }

    public static class WeightedAvg extends AggregateFunction<Long, WeightedAvgAccumulator> { // <T, ACC>
        @Override
        public WeightedAvgAccumulator createAccumulator() {
            return new WeightedAvgAccumulator();
        }

        @Override
        public Long getValue(WeightedAvgAccumulator acc) {
            if (acc.count == 0) {
                return null;
            } else {
                return (long) acc.sum / acc.count;
            }
        }

        // Processes the input values and updates the provided accumulator instance.
        public void accumulate(WeightedAvgAccumulator acc, Double iValue, Long iWeight) {
            acc.sum += iValue * iWeight;
            acc.count += iWeight;
        }

        // Retracts the input values from the accumulator instance.
        public void retract(WeightedAvgAccumulator acc, Double iValue, Long iWeight) {
            acc.sum -= iValue * iWeight;
            acc.count -= iWeight;
        }

        // Merges a group of accumulator instances into one accumulator instance.
        public void merge(WeightedAvgAccumulator acc, Iterable<WeightedAvgAccumulator> it) {
            for (WeightedAvgAccumulator a: it) {
                acc.count += a.count;
                acc.sum += a.sum;
            }
        }

//        public void resetAccumulator(WeightedAvgAccumulator acc) {
//            acc.count = 0L;
//            acc.sum = 0;
//        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);

        DataStream<Tuple5<String, Long, String, Double, Long>> input = env.addSource(new FixedInputGenerator().getGeneratorForTable(table.Input.orders));
        Table orders = tableEnv.fromDataStream(input, $("region"), $("user_id"), $("username"), $("value"), $("weight"));
        tableEnv.createTemporaryView("orders", orders);

        tableEnv.createTemporarySystemFunction("WeightedAvg", WeightedAvg.class);

        Table result = tableEnv.sqlQuery("SELECT region, WeightedAvg(`value`, weight) FROM orders GROUP BY region");
        tableEnv.toRetractStream(result, Row.class).print();

        env.execute();
    }
}

/*
(true,FRANCE,1000)
(true,US,50)
(false,FRANCE,1000) // retract
(true,FRANCE,325) // add
(true,CHINA,1000)
 */