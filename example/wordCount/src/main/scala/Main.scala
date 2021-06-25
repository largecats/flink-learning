import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.examples.wordcount.util.WordCountData

/**
  * Implements the "WordCount" program that computes a simple word occurrence
  * histogram over text files in a streaming fashion.
  *
  * /usr/local/flink-1.13.1_2.11/examples/batch/WordCount.jar
  *
  * The input is a plain text file with lines separated by newline characters.
  *
  * Usage:
  * {{{
  * WordCount --input <path> --output <path>
  * }}}
  *
  * If no parameters are provided, the program is run with default data from
  * {@link WordCountData}.
  *
  * This example shows how to:
  *
  *  - write a simple Flink Streaming program,
  *  - use tuple data types,
  *  - write and use transformation functions.
  *
  */
object Main {

  def main(args: Array[String]) {

    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    // get input data
    val text =
      // read the text file from given input path
      if(params.has("input")) {
        env.readTextFile(params.get("input"))
      }
      else {
        println("Executing WordCount example with default inputs data set.")
        println("Use --input to specify file input.")
        // get default test text data
        env.fromElements(WordCountData.WORDS: _*)
      }

    val counts: DataStream[(String, Int)] = text
    // split up the lines in pairs (2-tuples) containing: (word,1)
      .flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      // group by the tuple field "0" and sum up tuple field "1"
      .keyBy(_._1) // Scala tuple index is 1-based
      .sum(1)

    // emit result
    if(params.has("output")) {
      counts.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE)
    }
    else {
      println("Printing result to stdout. Use --output to specify output path.")
      counts.print()
    }

    // execute program
    env.execute("Streaming WordCount")
  }
}

/*
In WSL:
1. Build:
>sbt examples_wordCount/assembly
2. Start Flink cluster
:/usr/local/flink-1.12.2$ ./bin/start-cluster.sh
3. Run:
$ bash main.sh --input "/mnt/c/users/.../flink-learning/input/sample.txt" --output "/mnt/c/users/.../Desktop/output.txt"

In server (with YARN and HDFS):
1. Upload input/sample.txt to HDFS:
$ hadoop fs -put -f /input/sample.txt /user/<username>/flink/sample.txt
2. Build:
$ sbt examples_wordCount/assembly
3. Submit job to YARN (must supply --input):
$ bash main.sh -input hdfs://acluster/user/<username>/flink/sample.txt -output hdfs://acluster/user/<username>/flink/wordcount-result.txt
  If the wordcount-result.txt directory already exists, remove it

Inspect result in scala shell:
scala> val text = benv.readTextFile("hdfs://acluster/user/<username>/flink/wordcount-result.txt")
scala> text.print()
 */
