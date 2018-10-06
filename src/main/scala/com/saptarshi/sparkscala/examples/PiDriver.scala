package com.saptarshi.sparkscala.examples

import org.apache.spark.sql.SparkSession

object PiDriver {

    def main(args: Array[String]): Unit = {
        require(args != null && args.length == 1, "Sample size required")
        val Array(sampleSize) = args

        val spark = SparkSession.builder.appName("Pi").getOrCreate()

        val sampleRDD = spark.sparkContext.parallelize(1 to sampleSize.toInt)
        val filteredRDD = sampleRDD.filter(f => {
                val x = Math.random
                val y = Math.random
                x*x + y*y < 1
            }
        )

        println(s"Pi Approximation = ${ 4.0 * filteredRDD.count / sampleRDD.count }")
        spark.stop()
    }

}
