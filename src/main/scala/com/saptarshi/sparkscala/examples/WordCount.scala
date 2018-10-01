package com.saptarshi.sparkscala.examples

import org.apache.spark.sql.SparkSession

object WordCount {

    def main(args: Array[String]): Unit = {
        require(args != null && args.length == 1, "Provide the Input filename")
        val Array(filename) = args

        val spark = SparkSession.builder.appName("WordCount").getOrCreate()
        import spark.implicits._

        val lines = spark.read.textFile(filename)
        val wc = lines.flatMap(_.split(" ")).groupByKey(v => v).count()
        wc.collect.foreach(println)
    }

}
