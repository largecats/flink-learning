import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ListTypeInfo;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FindColorPattern {
    /*
    Given a stream of objects of different colors and shapes, want to find pairs of objects of the same color that have certain pattern, e.g., rectangle followed by triangle.
    One stream contains the objects, the other stream contains the rules.
    */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Item> itemStream = env.fromElements(
                new Item(new Color("red"), new Shape("rectangle")),
                new Item(new Color("red"), new Shape("rectangle")),
                new Item(new Color("red"), new Shape("rectangle")),
                new Item(new Color("red"), new Shape("rectangle"))
        );
        DataStream<Rule> ruleStream = env.fromElements(
                new Rule("two_rectangles", Tuple2.of(new Shape("rectangle"), new Shape("rectangle")))
        );
        // Since we want pairs of the same color, we can just key the object stream by color.
        KeyedStream<Item, Color> colorPartitionedStream = itemStream.keyBy(x -> x.color);

        // Map descriptor rule name -> rule, for creating broadcast state later
        MapStateDescriptor<String, Rule> ruleStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO, // Type of rule name
                TypeInformation.of(new TypeHint<Rule>() {})); // Type of rule itself

        // Broadcast the stream of rules and create the broadcast state where the rules are stored
        BroadcastStream<Rule> ruleBroadcastStream = ruleStream.broadcast(ruleStateDescriptor);

        // Evaluate rules against the object stream
        DataStream<String> output = colorPartitionedStream
                .connect(ruleBroadcastStream)
                .process(new PatternFinder());

        output.print();
        env.execute();
    }

    public static class PatternFinder extends KeyedBroadcastProcessFunction<Color, Item, Rule, String> { // Use BroadcastProcessFunction if the non-broadcasted stream (object stream) is not keyed
        // store partial matches, i.e. first elements of the pair waiting for their second element
        // we keep a list as we may have many first elements waiting
        private final MapStateDescriptor<String, List<Item>> mapStateDesc =
                new MapStateDescriptor<>(
                        "items",
                        BasicTypeInfo.STRING_TYPE_INFO,
                        new ListTypeInfo<>(Item.class));

        // identical to our ruleStateDescriptor above (so need to write twice?)
        private final MapStateDescriptor<String, Rule> ruleStateDescriptor =
                new MapStateDescriptor<>(
                        "RulesBroadcastState",
                        BasicTypeInfo.STRING_TYPE_INFO,
                        TypeInformation.of(new TypeHint<Rule>() {}));

        /*
        Add rule name and rule to broadcast state.
        When is this run?
         */
        @Override
        public void processBroadcastElement(Rule value,
                Context ctx,
//                KeyedBroadcastProcessFunction<Color, Item, Rule, String>.Context ctx, // KeyedBroadcastProcessFunction.Context is not the correct type, need to supply type parameter
                Collector<String> out) throws Exception {
            ctx.getBroadcastState(ruleStateDescriptor).put(value.name, value);
        }

        /*
        Called for each element in itemStream.
         */
        @Override
        public void processElement(Item value,
                ReadOnlyContext ctx,
                Collector<String> out) throws Exception {

            final MapState<String, List<Item>> state = getRuntimeContext().getMapState(mapStateDesc);
            final Shape shape = value.getShape();
            System.out.println("shape.value = " + shape.value);

            for (Map.Entry<String, Rule> entry :
                    ctx.getBroadcastState(ruleStateDescriptor).immutableEntries()) { // Empty at first run (which means processBroadcastElement() is not run yet?)
                final String ruleName = entry.getKey();
                System.out.println("ruleName = " + ruleName);
                final Rule rule = entry.getValue();
                System.out.println("rule.first.value = " + rule.first.value + ", rule.second.value = " + rule.second.value);

                List<Item> stored = state.get(ruleName);
                System.out.println("stored = " + stored);
                if (stored == null) {
                    stored = new ArrayList<>();
                }

                if (shape.equals(rule.second) && !stored.isEmpty()) {
                    for (Item i : stored) { // Why can match more than one first element?
                        System.out.print("match found");
                        out.collect("MATCH: " + i + " - " + value);
                    }
                    stored.clear();
                }

                // there is no else{} to cover if rule.first == rule.second
                System.out.println("shape.equals(rule.first) = " + shape.equals(rule.first));
                if (shape.equals(rule.first)) {
                    stored.add(value);
                }

                if (stored.isEmpty()) {
                    state.remove(ruleName);
                } else {
                    state.put(ruleName, stored);
                }
                System.out.println("stored = " + stored);
                System.out.println("");
            }
        }
    }
}

/* Issue 1
Why stored = null in each iteration?
Output:
shape.value = rectangle
shape.value = rectangle
ruleName = two_rectangles
rule.first.value = rectangle, rule.second.value = rectangle
stored = null
shape.equals(rule.first) = true
stored = [Item@60955147]

shape.value = rectangle
ruleName = two_rectangles
rule.first.value = rectangle, rule.second.value = rectangle
stored = null
shape.equals(rule.first) = true
stored = [Item@1d436a4f]

shape.value = rectangle
ruleName = two_rectangles
rule.first.value = rectangle, rule.second.value = rectangle
stored = null
shape.equals(rule.first) = true
stored = [Item@16937fb2]
 */

/*
Issue 2
The order of the items printed is different from the order of items in env.fromElements().
Can change to different shapes and/or keys to observe.
 */