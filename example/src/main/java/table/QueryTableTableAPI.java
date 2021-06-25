package table;

import common.sources.FixedInputGenerator;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import static org.apache.flink.table.api.Expressions.$;

public class QueryTableTableAPI {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        DataStream<Tuple5<String, Long, String, Double, Long>> input = env.addSource(new FixedInputGenerator().getGeneratorForTable(Input.orders));
        Table orders = tableEnv.fromDataStream(input, $("cCountry"), $("cID"), $("cName"), $("revenue"));

        // compute revenue by country
        Table revenue = orders
            .groupBy($("cCountry"))
                .select($("cCountry"), $("revenue").sum().as("revSum"));

        // Converts the given Table into a DataStream of add and retract messages
        tableEnv.toRetractStream(revenue, TypeInformation.of(new TypeHint<Tuple2<String, Double>>(){})).print();
//        tableEnv.toRetractStream(revenue, Row.class).print(); // Printed output will be like (true,FRANCE,1000.0)

        env.execute();
    }
}

/*
Output format: (boolean message flag, tuple). Message flag indicates whether this is a new record to INSERT or an old record to DELETE ("retract").

1> (true,(FRANCE,1000.0)) // INSERT new record
7> (true,(CHINA,1000.0)) // INSERT new record
1> (true,(US,50.0)) // INSERT new record
1> (false,(FRANCE,1000.0)) // DELETE old record
1> (true,(FRANCE,1190.0)) // INSERT new record

The update operation is decomposed into retracting the old record and adding a new record.
See https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/streaming/dynamic_tables.html#table-to-stream-conversion.
 */
