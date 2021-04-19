import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import common.datatypes._
import common.sources.ItemGenerator

/*
Scala version of FindColorPattern.
 */
object BroadcastState1 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    val items = env.addSource(new ItemGenerator());
    val items = env.fromElements(
      new Item(new Color("red"), new Shape("rectangle")),
      new Item(new Color("red"), new Shape("rectangle")),
      new Item(new Color("red"), new Shape("rectangle")),
      new Item(new Color("red"), new Shape("rectangle"))
    )
    val rules = env.fromElements(
      new Rule("two_rectangles", new Shape("rectangle"), new Shape("rectangle"))
    )
//    val keyedItems = items.keyBy(x => x.color);
    val keyedItems = items.keyBy(x => x.color.value);
    val ruleStateDescriptor = new MapStateDescriptor[String, Rule](
      "RuleBroadcastState",
      classOf[String], classOf[Rule]
    )
    val broadcastedRules = rules.broadcast(ruleStateDescriptor)
    class PatternFinder extends KeyedBroadcastProcessFunction[Color, Item, Rule, String] {
      private val mapStateDesc = new MapStateDescriptor[String, List[Item]]("itemsState", classOf[String], classOf[List[Item]])
      private val ruleStateDescriptor = new MapStateDescriptor[String, Rule]("RuleBroadcastState", classOf[String], classOf[Rule])

      override def processBroadcastElement(value: Rule, ctx: KeyedBroadcastProcessFunction[Color, Item, Rule, String]#Context, out: Collector[String]): Unit = {
        ctx.getBroadcastState(ruleStateDescriptor).put(value.name, value)
      }

      override def processElement(value: Item, ctx: KeyedBroadcastProcessFunction[Color, Item, Rule, String]#ReadOnlyContext, out: Collector[String]): Unit = {
        val state = getRuntimeContext.getMapState(mapStateDesc)
        val shape = value.getShape
        // println(s"shape.value = ${shape.value}")

        import collection.JavaConverters._
        for (entry <- ctx.getBroadcastState(ruleStateDescriptor).immutableEntries().asScala) {
          val ruleName = entry.getKey
          // println(s"ruleName = $ruleName")
          val rule = entry.getValue
          // println(s"rule.first.value = ${rule.first.value}, rule.second.value = ${rule.second.value}")

          var stored = state.get(ruleName)
          // println(s"stored = $stored")
          if (stored == null) {
            stored = List()
          }

          // println(s"shape.equals(rule.second) && !stored.isEmpty = ${shape.equals(rule.second) && !stored.isEmpty}")
          if (shape.equals(rule.second) && !stored.isEmpty) {
            for (i <- stored) {
              out.collect(s"MATCH color: ${i.color.value}, shape: ${i.shape.value} - color: ${value.color.value}, shape: ${value.shape.value}")
            }
            stored = List()
          }

          // println(s"shape.equals(rule.first) = ${shape.equals(rule.first)}")
          if (shape.equals(rule.first)) {
            stored = value +: stored
          }
          if (stored.isEmpty) {
            state.remove(ruleName)
          } else {
            state.put(ruleName, stored)
          }
          // println(s"stored = ${stored}")
          // println("")
        }
      }
    }
    val output = keyedItems.connect(broadcastedRules).process(new PatternFinder)
    output.print();
    env.execute();
  }

}
