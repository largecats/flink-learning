import common.datatypes.RateSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import java.util.{Arrays, Properties}
import org.apache.kafka.clients.producer.ProducerRecord

import java.lang
import java.nio.charset.StandardCharsets
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}

object Main {

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val consumerProperties = new Properties()
    consumerProperties.setProperty("bootstrap.servers", "data-kafka00:8920")

    val myConsumer =
      new FlinkKafkaConsumer(Arrays.asList("test0"), new SimpleStringSchema(), consumerProperties)
    myConsumer.setStartFromLatest()
    val input: DataStream[String] = env.addSource(myConsumer)
    input.print()

    val output: DataStream[String] = input

    val producerProperties = new Properties()
    producerProperties.setProperty("bootstrap.servers", "data-kafka00:8920")

    val myProducer = new FlinkKafkaProducer[String](
      "test",
      new SplitTopicKafkaSerializationSchema(),
      producerProperties,
      FlinkKafkaProducer.Semantic.EXACTLY_ONCE
    )

    output.addSink(myProducer)

    env.execute("SplitKafkaTopic")

  }

  class SplitTopicKafkaSerializationSchema extends KafkaSerializationSchema[String] {
    override def serialize(element: String, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
      println(s"element = ${element}")
      val record = parse(element).extract[RateSchema] // schema needs to be Scala case class for json4s to parse
      val key = record.id.toString
      if(record.id % 2 == 0) {
        val outputElement = write(
          OutputSchemaEven(
            record.id,
            record.timestamp
          )
        )
        println(s"outputElement = ${outputElement}")
        new ProducerRecord[Array[Byte], Array[Byte]](
          "test2",
          key.getBytes(StandardCharsets.UTF_8),
          outputElement.getBytes(StandardCharsets.UTF_8)
        )
      }
      else {
        val outputElement = write(
          OutputSchemaOdd(
            record.id
          )
        )
        println(s"outputElement = ${outputElement}")
        new ProducerRecord[Array[Byte], Array[Byte]](
          "test1",
          key.getBytes(StandardCharsets.UTF_8),
          outputElement.getBytes(StandardCharsets.UTF_8)
        )
      }
    }
  }
}

/*
Topic "test0":
3> {"id":0,"timestamp":1622736290032}
3> {"id":1,"timestamp":1622736291032}
3> {"id":2,"timestamp":1622736292032}
  ...
3> {"id":98,"timestamp":1622736388032}
3> {"id":99,"timestamp":1622736389032}

Topic "test1":
2> {"id":1}
2> {"id":3}
2> {"id":5}
2> {"id":7}
  ...
2> {"id":97}
2> {"id":99}

Topic "test2":
1> {"id":0,"timestamp":1622736290032}
1> {"id":2,"timestamp":1622736292032}
1> {"id":4,"timestamp":1622736294032}
1> {"id":6,"timestamp":1622736296032}
1> {"id":8,"timestamp":1622736298032}
  ...
1> {"id":96,"timestamp":1622736386032}
1> {"id":98,"timestamp":1622736388032}

Topic "test" is empty
 */
