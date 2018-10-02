package com.saptarshi.sparkscala.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

object WordCount {

    def main(args: Array[String]): Unit = {
        require(args != null && args.length == 2, "Input filename and Method (DS/DF/RDD) required")
        val Array(filename, method) = args

        val spark = SparkSession.builder.appName("WordCount").getOrCreate()
        import spark.implicits._

        if (method == "DS") {    // Using Spark Dataset
            val lines = spark.read.text(filename).as[String]
            val wc = lines.flatMap(_.split(" ")).groupByKey(v => v).count()
            wc.collect.foreach(println)
        }
        else if (method == "DF") {    // Using Spark DataFrame
            val lines = spark.read.text(filename)
            val wc = lines.flatMap(_.getString(0).split(" ")).groupByKey(v => v).count()
            wc.collect.foreach(println)
        }
        else {    // Using Spark RDD
            val lines = spark.read.text(filename).as[String].rdd
            val wc = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
            wc.collect.foreach(println)
        }

        spark.stop()
    }

}
