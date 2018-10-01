package com.saptarshi.sparkscala.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

object WordCount {

    def main(args: Array[String]): Unit = {
        require(args != null && args.length == 2, "Input filename and Method (DS/DF) required")
        val Array(filename, method) = args

        val spark = SparkSession.builder.appName("WordCount").getOrCreate()
        import spark.implicits._

        var wc: Dataset[(String, Long)] = null

        if (method == "DS") {    // Using Spark Dataset
            val lines = spark.read.text(filename).as[String]
            wc = lines.flatMap(_.split(" ")).groupByKey(v => v).count()
        }
        else {    // Using Spark DataFrame
            val lines = spark.read.text(filename)
            wc = lines.flatMap(_.getString(0).split(" ")).groupByKey(v => v).count()
        }

        wc.collect.foreach(println)
    }

}
