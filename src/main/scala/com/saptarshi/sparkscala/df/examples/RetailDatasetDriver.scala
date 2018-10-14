package com.saptarshi.sparkscala.df.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._

object RetailDatasetDriver {

    def main(args: Array[String]): Unit = {
        require(args != null && args.length == 1, "Retail Datafile required")
        val Array(retailFile) = args

        val spark = SparkSession.builder.appName("Retail Dataset").getOrCreate()
        import spark.implicits._

        val input = spark.read
                    .option("delimiter", "\t")
                    .option("dateFormat", "yyyy-MM-dd")
                    .option("timestampFormat", "HH:mm")
                    .schema(Encoders.product[RetailData].schema)
                    .csv(retailFile)

        val res1 = input.groupBy("product").agg(sum("sales") as "tot_sales", count("sales") as "count_sales")
        val res2 = input.groupBy().agg(sum("sales") as "grand_total_sales", count("sales") as "grand_total_count")

        res1.collect().foreach(println)
        res2.collect().foreach(println)

        spark.stop()
    }

}
