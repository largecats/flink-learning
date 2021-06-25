package table.udf;

import common.sources.FixedInputGenerator;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class TableAggregateFunctionUdf {

    public static class Top2Accumulator {
        public Double first = 0.0;
        public Double second = 0.0;
    }

    public static class Top2 extends TableAggregateFunction<Tuple2<Double, Integer>, Top2Accumulator> {
        @Override
        public Top2Accumulator createAccumulator() {
            return new Top2Accumulator();
        }

        public void accumulate(Top2Accumulator acc, Double value) {
            if (value > acc.first) {
                acc.second = acc.first;
                acc.first = value;
            } else if (value > acc.second) {
                acc.second = value;
            }
        }

        public void merge(Top2Accumulator acc, Iterable<Top2Accumulator> it) {
            for (Top2Accumulator otherAcc: it) {
                accumulate(acc, otherAcc.first);
                accumulate(acc, otherAcc.second);
            }
        }

        public void emitValue(Top2Accumulator acc, Collector<Tuple2<Double, Integer>> out) {
            System.out.println("acc.first = " + acc.first);
            System.out.println("acc.second = " + acc.second);
            if (acc.first != 0.0) {
                out.collect(Tuple2.of(acc.first, 1));
            }
            if (acc.second != 0.0) {
                out.collect(Tuple2.of(acc.second, 2));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);

        DataStream<Tuple5<String, Long, String, Double, Long>> input = env.addSource(new FixedInputGenerator().getGeneratorForTable(table.Input.orders));
        Table orders = tableEnv.fromDataStream(input, $("region"), $("user_id"), $("username"), $("price"), $("quantity"));
        tableEnv.createTemporaryView("orders", orders);

        tableEnv.createTemporarySystemFunction("Top2", Top2.class);

        // Find the top 2 prices for each region
        Table result = orders.groupBy($("region"))
                .flatAggregate(call("Top2", $("price")).as("price", "rank"))
                .select($("*"));

        tableEnv.toRetractStream(result, Row.class).print();

        env.execute();
    }
}

/*
(true,FRANCE,1000.0,1)
(true,US,50.0,1)
(false,FRANCE,1000.0,1)
(true,FRANCE,1000.0,1) // Why is this record inserted again after it was retracted? See below
(true,FRANCE,190.0,2)
(true,CHINA,1000.0,1)

The insert after retract is actually updating both of the top 2 prices; it's just that the second highest price wasn't emitted
because it didn't exist.
acc.first = 1000.0
acc.second = 0.0
(true,FRANCE,1000.0,1) // Only the highest price is emitted
acc.first = 50.0
acc.second = 0.0
(true,US,50.0,1)  // Only the highest price is emitted
acc.first = 1000.0
acc.second = 0.0
(false,FRANCE,1000.0,1) // To update the previous emit which contains only the highest price, first retract it
acc.first = 1000.0
acc.second = 190.0
(true,FRANCE,1000.0,1) // And insert the updated emit, this time containing a second highest price
(true,FRANCE,190.0,2)
acc.first = 1000.0
acc.second = 0.0
(true,CHINA,1000.0,1) // Only the highest price is emitted
 */