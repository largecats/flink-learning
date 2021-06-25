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
import table.Input;

import static org.apache.flink.table.api.Expressions.*;

public class TableAggregateFunctionUdfWithRetract {

    public static class Top2WithRetractAccumulator {
        public Double first = Double.MIN_VALUE;
        public Double second = Double.MIN_VALUE;
        public Double oldFirst = Double.MIN_VALUE;
        public Double oldSecond = Double.MIN_VALUE;
    }

    public static class Top2WithRetract extends TableAggregateFunction<Tuple2<Double, Integer>, Top2WithRetractAccumulator> {
        @Override
        public Top2WithRetractAccumulator createAccumulator() {
            return new Top2WithRetractAccumulator();
        }

        public void accumulate(Top2WithRetractAccumulator acc, Double value) {
            if (value > acc.first) {
                acc.second = acc.first;
                acc.first = value;
            } else if (value > acc.second) {
                acc.second = value;
            }
        }

        // Only emit when there is an update
        public void emitUpdateWithRetract(
                Top2WithRetractAccumulator acc,
                RetractableCollector<Tuple2<Double, Integer>> out
        ) {
            if (!acc.first.equals(acc.oldFirst)) {
                if (acc.oldFirst != Double.MIN_VALUE) { // if acc.oldFirst == Double.MIN_VALUE, then it hasn't been emitted, so don't need to retract
                    out.retract(Tuple2.of(acc.oldFirst, 1));
                }
                out.collect(Tuple2.of(acc.first, 1));
                acc.oldFirst = acc.first;
            }
            if (!acc.second.equals(acc.oldSecond)) {
                if (acc.oldSecond != Double.MIN_VALUE) {
                    out.retract(Tuple2.of(acc.oldSecond, 2));
                }
                out.collect(Tuple2.of(acc.second, 2));
                acc.oldSecond = acc.second;
            }


        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);

        DataStream<Tuple5<String, Long, String, Double, Long>> input = env.addSource(new FixedInputGenerator().getGeneratorForTable(table.Input.orders));
        Table orders = tableEnv.fromDataStream(input, $("region"), $("user_id"), $("username"), $("price"), $("quantity"));
        tableEnv.createTemporaryView("orders", orders);

        tableEnv.createTemporarySystemFunction("Top2WithRetract", Top2WithRetract.class);

        Table result = orders.groupBy($("region"))
                .flatAggregate(call("Top2WithRetract", $("price")).as("price", "rank"))
                .select($("*"));

        tableEnv.toRetractStream(result, Row.class).print();

        env.execute();
    }
}

/*
Using Blink planner:
Exception in thread "main" org.apache.flink.table.api.ValidationException: Could not find an implementation method 'emitValue' in class 'class table.udf.TableAggregateFunctionUdfWithRetract$Top2WithRetract' for function 'Top2WithRetract' that matches the following signature:
void emitValue(table.udf.TableAggregateFunctionUdfWithRetract.Top2WithRetractAccumulator, org.apache.flink.util.Collector)

Using old planner:
(true,FRANCE,1000.0,1)
(true,US,50.0,1)
(true,FRANCE,190.0,2)
(true,CHINA,1000.0,1)
 */