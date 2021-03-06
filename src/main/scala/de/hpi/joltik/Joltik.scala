package de.hpi.joltik

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.rogach.scallop.{ScallopConf, ScallopOption}

class CommandLineConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val path: ScallopOption[String] = opt[String](default = Some("./TPCH"), descr = "Path to CSV files")
  val cores: ScallopOption[Int] = opt[Int](required = true, default = Some(4), descr = "Number of cores")
  verify()
}
object SimpleSpark extends App {

  override def main(args: Array[String]): Unit = {

    val commandLineConf = new CommandLineConf(args)

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    //------------------------------------------------------------------------------------------------------------------
    // Setting up a Spark Session
    //------------------------------------------------------------------------------------------------------------------

    // Create a SparkSession to work with Spark
    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master(s"local[${commandLineConf.cores()}]") // local, with 4 worker cores
    val spark = sparkBuilder.getOrCreate()

    // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
    spark.conf.set("spark.sql.shuffle.partitions", s"${commandLineConf.cores() * 2}") //

    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types
    import spark.implicits._

    def time[R](block: => R): R = {
      val t0 = System.currentTimeMillis()
      val result = block
      val t1 = System.currentTimeMillis()
      println(s"Execution: ${t1 - t0} ms")
      result
    }

    //------------------------------------------------------------------------------------------------------------------
    // Inclusion Dependency Discovery (Homework)
    //------------------------------------------------------------------------------------------------------------------

    val inputs = List("region", "nation", "supplier", "customer", "part", "lineitem", "orders")
      .map(name => s"${commandLineConf.path()}/tpch_$name.csv")

    time {Sindy.discoverINDs(inputs, spark)}
  }
}
