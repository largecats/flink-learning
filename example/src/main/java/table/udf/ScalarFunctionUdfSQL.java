package table.udf;

import common.sources.FixedInputGenerator;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class ScalarFunctionUdfSQL {

    public static class SubStringFunction extends ScalarFunction {
        public String eval(String s, Integer begin, Integer end) {
            return s.substring(begin, end);
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

        tableEnv.createTemporaryFunction("SubstringFunction", SubStringFunction.class);
        Table result = tableEnv.sqlQuery("SELECT SubstringFunction(region, 0, 2) FROM orders");
        tableEnv.toAppendStream(result, Row.class).print();

        env.execute();
    }
}

/*
FR
US
FR
CH
 */
