import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class Accumulator {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> data = env.fromElements("Wake", "up", "Neo", "The", "Matrix", "has", "you");
        DataStream<String> result = data
                .map(new Counter())
                .setParallelism(1);
        JobExecutionResult jobResult = env.execute();
        System.out.println("Count = " + jobResult.getAccumulatorResult("counter"));

    }

    public static class Counter extends RichMapFunction<String, String> {
        private IntCounter count = new IntCounter(); // Create IntCounter
        private int regularCount = 0; // If parallelism = 1, then regularCount result == IntCounter result; else, regularCount counts the words processed by each parallel instance of the operator separately

        @Override
        public void open(Configuration parameters) throws Exception {
            getRuntimeContext().addAccumulator("counter", this.count);

        }

        @Override
        public String map(String value) throws Exception {
            this.count.add(1);
            regularCount++;
            System.out.println("Subtask " + getRuntimeContext().getIndexOfThisSubtask() + ": regularCount = " + regularCount);
            return value; // Trivial dummy map
        }
    }
}

/*
Parallelism = 4
Subtask 0: regularCount = 1
Subtask 1: regularCount = 1
Subtask 3: regularCount = 1
Subtask 2: regularCount = 1
Subtask 3: regularCount = 2
Subtask 0: regularCount = 2
Subtask 2: regularCount = 2
Count = 7

Parallelism = 1
Subtask 0: regularCount = 1
Subtask 0: regularCount = 2
Subtask 0: regularCount = 3
Subtask 0: regularCount = 4
Subtask 0: regularCount = 5
Subtask 0: regularCount = 6
Subtask 0: regularCount = 7
Count = 7

 */