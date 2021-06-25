package table.udf;

import common.sources.FixedInputGenerator;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class TableFunctionUdfSQL {
    @FunctionHint(output = @DataTypeHint("ROW<word String, LENGTH int>"))
    public static class SplitFunction extends TableFunction<Row> {
        public void eval(String str) {
            for (String s: str.split(" ")) { // split by space
                collect(Row.of(s, s.length())); // returns a row
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

        tableEnv.createTemporaryFunction("SplitFunction", SplitFunction.class);

//        Table result = tableEnv.sqlQuery("SELECT * FROM orders, LATERAL TABLE(SplitFunction(region))");
        Table result = tableEnv.sqlQuery("SELECT * FROM orders LEFT JOIN LATERAL TABLE(SplitFunction(region)) ON TRUE");
        tableEnv.toAppendStream(result, Row.class).print();

        env.execute();
    }
}

/*
FRANCE,10001,amy,1000.0,0,FRANCE,6
US,502,bob,50.0,3,US,2
FRANCE,401,frank,190.0,5,FRANCE,6
CHINA,101,xiaoming,1000.0,6,CHINA,5
 */
