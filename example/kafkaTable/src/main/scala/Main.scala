import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.factories.FactoryUtil.discoverFactory

object Main {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    val tableEnv = StreamTableEnvironment.create(env, settings);
    env.setParallelism(1);

    // Create connector table
    tableEnv.executeSql(
      "CREATE TABLE Orders (`user_id` BIGINT, `price` DOUBLE, `quantity` INTEGER, `cost` AS price * quantity) WITH ('connector' = 'kafka', 'topic' = 'test', 'properties.bootstrap.servers' = 'data-kafka00:8920', 'scan.startup.mode' = 'earliest-offset', 'format' = 'csv')"
    );
    tableEnv.executeSql(
      "INSERT INTO Orders VALUES " +
        "(1001, 100.0, 2)," +
        "(1003, 50.0, 10)"
    );
  }
}
/*
Check whether flink kafka connector is included in assembled jar classes: jar tvf <yourjar.jar> |grep -i kafka.

Add jar to scala shell via yt: /bin/flink run -m yarn-cluster yt /home/datadev/xiaolf/flink-connector-kafka_2.11-1.13.1.jar target/scala-2.11/example_kafkaTable.jar
 */
