package com.saptarshi.sparkscala.examples

import org.apache.spark.sql.SparkSession

object MapPartitionsDriver {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder.appName("Map Partitions").getOrCreate()

        import spark.implicits._

        val input = spark.sparkContext.parallelize(
                List("red", "green", "blue", "yellow", "cyan", "magenta", "black", "white"), 3
            ).toDS()

        val output = input.mapPartitions(partition => {
                val str = partition.toList.mkString("[", ":", "]")
                List(str).iterator
            }
        )

        output.collect.foreach(println)

        spark.stop()
    }

}
