import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.scala.createTypeInformation
import common.sources.BoundedRateGenerator
import common.datatypes.Rate
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import java.util.concurrent.TimeUnit
import org.apache.flink.api.common.typeinfo.TypeInformation

import collection.JavaConverters._

object Main {

  def main(args: Array[String]): Unit = {
    val parameter: ParameterTool = ParameterTool.fromArgs(args) // e.g., --size 100 --errorOn 77 --checkIntervalMs 10000
    val size: Integer = parameter.get("size","100").toInt
    val errorOn: Integer = parameter.get("errorOn",size.toString).toInt
    val checkInterval: Long = parameter.get("checkIntervalMs","10000").toLong
    val toMergeParameter: ParameterTool = ParameterTool.fromMap(Map("size"->size.toString,"errorOn"->errorOn.toString,"checkIntervalMs"->checkInterval.toString).asJava)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
      3, // number of restart attempts
      Time.of(5, TimeUnit.SECONDS) // delay
    ))
    env.getConfig.setGlobalJobParameters(parameter.mergeWith(toMergeParameter))
    env.enableCheckpointing(checkInterval)
    //val integerInfo: TypeInformation[Integer] = createTypeInformation[Integer]
    //val rateInfo: TypeInformation[Rate] = createTypeInformation[Rate]
    val input: DataStream[Rate] = env.addSource(new BoundedRateGenerator(size))
    val keyedInput: KeyedStream[Rate,Integer] = input.keyBy(x => x.id % 3)
    val result: DataStream[Rate] = keyedInput.process(new ThrowErrorOn)
    result.print()
    env.execute("HA Fail Fast")
  }
  class ThrowErrorOn extends KeyedProcessFunction[Integer, Rate, Rate] {
    private lazy val parameters = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    private lazy val errorOn = parameters.getRequired("errorOn").toInt
    private lazy val size = parameters.getRequired("size").toInt
    override def processElement(value: Rate,
                                ctx: KeyedProcessFunction[Integer, Rate, Rate]#Context,
                                out: Collector[Rate]): Unit = {
      if (errorOn < size && errorOn.equals(value.id)) {
        throw new RuntimeException(s"errorOn: $errorOn")
      } else {
        out.collect(value)
      }
    }
  }
}
