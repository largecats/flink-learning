import java.lang
import java.nio.charset.StandardCharsets

import common.sources.{BoundedRateGenerator, RateGenerator}
import common.datatypes.{Rate, RateSchema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.flink.api.java.tuple.Tuple2
import java.util.Properties

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.connectors.kafka.internals.{
  KafkaSerializationSchemaWrapper,
  KeyedSerializationSchemaWrapper
}
import org.apache.kafka.clients.producer.ProducerRecord
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}

object Main {

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def main(args: Array[String]): Unit = {
    val parameters = ParameterTool.fromArgs(args) // e.g., --topic test --server data-kafka00:8920
    val topic = parameters.get("topic")
    val server = parameters.get("server")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val properties = new Properties()
//        properties.setProperty("bootstrap.servers", "172.24.29.59:9092") // output of hostname -I in WSL
    properties.setProperty("bootstrap.servers", server)

    val stream: DataStream[RateSchema] =
      env.addSource(new BoundedRateGenerator()).map((x: Rate) => RateSchema(x.id, x.timestamp))

    class RateSerializationSchema extends KafkaSerializationSchema[RateSchema] {
      override def serialize(element: RateSchema, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        val outputElement = write(element)
        new ProducerRecord[Array[Byte], Array[Byte]](
          topic,
          element.id.toString.getBytes(StandardCharsets.UTF_8),
          outputElement.getBytes(StandardCharsets.UTF_8)
        )
      }
    }

    val myProducer = new FlinkKafkaProducer[RateSchema](
      topic,
      new RateSerializationSchema(),
      properties,
      FlinkKafkaProducer.Semantic.EXACTLY_ONCE
    )

    stream.addSink(myProducer)

    env.execute("KafkaProducer")

  }

}

/*
In WSL:
Caused by: org.apache.kafka.common.errors.TimeoutException: Topic my-topic not present in metadata after 60000 ms.

In server:
After running KafkaProducer, run KafkaConsumer with topic "test" to verify.
 */
