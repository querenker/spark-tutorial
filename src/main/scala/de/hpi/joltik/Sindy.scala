package de.hpi.joltik

import org.apache.spark.sql.{Row, SparkSession}

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    import spark.implicits._

    val datasets = inputs.map(
      spark.read
        .option("inferSchema", "false")
        .option("header", "true")
        .option("sep", ";")
        .csv(_)
    )

    val cells = datasets.map(dataSet => {
      val datasetColumns = dataSet.columns
      dataSet.flatMap(row => row.toSeq.asInstanceOf[Seq[String]].zip(datasetColumns))
    }).reduce(_ union _).distinct()

    val attributeSets = cells.rdd.groupByKey().mapValues(_.toSet).values.distinct()

    val inclusionLists = attributeSets.flatMap(attributeSet =>
      attributeSet.map(x => (x , attributeSet - x))
    )

    val results = inclusionLists.reduceByKey(_ intersect _).filter(_._2.nonEmpty)

    results.foreach(resultPair => println(s"${resultPair._1} < ${resultPair._2.mkString(", ")}"))
  }
}
