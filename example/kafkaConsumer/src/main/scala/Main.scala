import java.lang.Exception

import common.datatypes.RateSchema
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import java.util.Arrays
import java.util.Properties

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}

object Main {

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def main(args: Array[String]): Unit = {
    val parameter = ParameterTool.fromArgs(args) // e.g., --topic test --server data-kafka00:8920
    val topic = parameter.get("topic")
    val server = parameter.get("server")
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.setParallelism(1)
    val properties = new Properties()
//        properties.setProperty("bootstrap.servers", "172.24.29.59:9092") // office laptop WSL
    // properties.setProperty("bootstrap.servers", "172.29.24.74:9092") // home laptop WSL
    properties.setProperty("bootstrap.servers", server)

    val myConsumer =
      new FlinkKafkaConsumer(Arrays.asList(topic), new SimpleStringSchema(), properties)
//    myConsumer.setStartFromEarliest()
    myConsumer.setStartFromLatest()
    val stream: DataStream[String] = env
      .addSource(myConsumer)
    stream.print()
//    val parsedStream: DataStream[RateSchema] = stream.map(
//      new MapFunction[String, RateSchema] {
//        override def map(value: String): RateSchema = {
//          val rate = parse(value).extract[RateSchema]
//          rate
//        }
//      }
//    )
//    parsedStream.print()

    env.execute("KafkaConsumer")

  }
}

/*
In WSL:
Caused by: org.apache.kafka.common.errors.TimeoutException: Timeout of 60000ms expired before the position for partition quickstart-events-0 could be determined

In server:
Run using bash main.sh --topic test --server "data-kafka00:8920"
The output of stream.print() is in taskmanager.out, because Flink treats DataStream's .print() like sink, and sink is executed at TaskManagers.
This is different from DataSet's print(): https://ci.apache.org/projects/flink/flink-docs-release-1.13/api/java/org/apache/flink/api/java/DataSet.html#print--
So to view the output of stream.print() in YARN UI, go to application >> attempt >> Log of containers whose ids end in 000002, 000003, etc (000001 is JobManager) >> taskmanager.out.

 */
