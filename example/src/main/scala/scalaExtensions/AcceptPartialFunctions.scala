package scalaExtensions

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions._
import org.apache.flink.streaming.api.scala.extensions.acceptPartialFunctions
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object AcceptPartialFunctions {

  case class Point(x: Double, y: Double)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(Point(1, 2), Point(3, 4), Point(5, 6))
    val ds1 = ds.filterWith {
      case Point(x, _) => x > 1
    }
      .keyBy(x => 0) // dummy key
    println(ds1.executeAndCollect().toList)
    Thread.sleep(5000);
    val ds2 = ds1
      .reduceWith {
      case (Point(x1, y1), (Point(x2, y2))) => Point(x1 + y1, x2 + y2)
    }
    println(ds2.executeAndCollect().toList)
    Thread.sleep(5000);
    val ds3 = ds2
      .mapWith {
      case Point(x, y) => (x, y)
    }
    println(ds3.executeAndCollect().toList)
    Thread.sleep(5000);
    val ds4 = ds3
      .flatMapWith {
      case (x, y) => Seq("x" -> x, "y" -> y) // returns Seq(("x", x), ("y", y)) for (x, y)
    }
//    ds4.print();
    println(ds4.executeAndCollect().toList)
//    env.execute();
  }
}

/*
Without Thread.sleep():
List(Point(3.0,4.0), Point(5.0,6.0))
List(Point(3.0,4.0), Point(7.0,11.0))
List((3.0,4.0), (7.0,11.0))
List((x,5.0), (y,6.0), (x,11.0), (y,7.0)) // The first two tuples are not (x, 3.0), (y, 4.0)

With Thread.sleep():
executeAndCollect() is executed asynchronously. To avoid messing up the order of elements, add Thread.sleep() in between each println().
List(Point(5.0,6.0), Point(3.0,4.0))
List(Point(3.0,4.0), Point(7.0,11.0))
List((3.0,4.0), (7.0,11.0))
List((x,3.0), (y,4.0), (x,7.0), (y,11.0))
 */