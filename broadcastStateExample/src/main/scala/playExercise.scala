/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.typeinfo.{TypeHint,TypeInformation,BasicTypeInfo}
import org.apache.flink.api.common.state.{MapState,MapStateDescriptor}
import common.sources.{TaxiFareGenerator,TaxiRideGenerator}
import common.datatypes.{TaxiFare,TaxiRide}
import common.utils.ExerciseBase._
import common.utils.{ExerciseBase, MissingSolutionException}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.api.java.typeutils.ListTypeInfo
import org.apache.flink.util.Collector

/**
  * The "Hourly Tips" exercise of the Flink training in the docs.
  *
  * The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
  * then from that stream, find the highest tip total in each hour.
  *
  */
case class Rule1(
                 name: String,
                 first: String,
                 second: String
               )

object playExercise {

  def main(args: Array[String]) {

    // set up streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(ExerciseBase.parallelism)

    // start the data generator
    val fares = env.addSource(fareSourceOrTest(new TaxiFareGenerator()))
    val rides = env.addSource(rideSourceOrTest(new TaxiRideGenerator()))
    val keyedFares = fares.keyBy(fare => fare.taxiId)
    // print result on stdout
    //printOrTest(rides)
    val rules = env.fromElements(new Rule1("test_rule","CASH","CARD"))
    val  ruleStateDescriptor: MapStateDescriptor[String, Rule1] = new MapStateDescriptor[String,Rule1](
      "Rule1sBroadcastState",
      classOf[String], classOf[Rule1])
    val broadcastedRule1s = rules.broadcast(ruleStateDescriptor)

    class myProcess extends KeyedBroadcastProcessFunction[Long, TaxiFare, Rule1, String] {
      private val mapStateDesc = new MapStateDescriptor[String, List[TaxiFare]]("faresState", classOf[String], classOf[List[TaxiFare]])

      // identical to our ruleStateDescriptor above
      private val ruleStateDescriptor = new MapStateDescriptor[String, Rule1]("Rule1sBroadcastState", classOf[String], classOf[Rule1])

      override def processBroadcastElement(value: Rule1, ctx: KeyedBroadcastProcessFunction[Long, TaxiFare, Rule1, String]#Context, out: Collector[String]): Unit = {
        ctx.getBroadcastState(ruleStateDescriptor).put(value.name, value)
      }

      override def processElement(value: TaxiFare, ctx: KeyedBroadcastProcessFunction[Long, TaxiFare, Rule1, String]#ReadOnlyContext, out: Collector[String]): Unit = {
        val state = getRuntimeContext.getMapState(mapStateDesc)
        val paymentType = value.paymentType
        import collection.JavaConverters._
        for (entry <- ctx.getBroadcastState(ruleStateDescriptor).immutableEntries.asScala) {
          val ruleName = entry.getKey
          val rule = entry.getValue
          var stored = state.get(ruleName)
          if (stored == null) {
            stored = List()
          }
          if (( paymentType == rule.second) && !stored.isEmpty) {
            for (i <- stored) {
              out.collect("MATCH: taxiId: " + value.taxiId + " " + i.paymentType + " - " + paymentType)
            }
            stored = List()
          }
          // there is no else{} to cover if rule.first == rule.second
          if (paymentType == rule.first) {
            stored = value +: stored
          }
          if (stored.isEmpty) {
            state.remove(ruleName)
          } else {
            state.put(ruleName, stored)
          }
        }
      }
    }
    val output = keyedFares.connect(broadcastedRule1s).process(new myProcess)

    printOrTest(output)
    // execute the transformation pipeline
    env.execute("Play Ground (scala)")
  }

}
