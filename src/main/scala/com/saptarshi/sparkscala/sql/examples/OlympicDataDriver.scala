package com.saptarshi.sparkscala.sql.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ StructType, StructField, StringType }

object OlympicDataDriver {

    def main(args: Array[String]): Unit = {
        require(args != null && args.length == 1, "Olympic Datafile required")
        val Array(olympicFile) = args

        val spark = SparkSession.builder.appName("Olympic Data Analysis").getOrCreate()
        import spark.implicits._

        val headers = Array("athlete", "age", "country", "year", "closing_ceremony_date", "sport", "gold", "silver", "bronze", "total")
        val schema = StructType( for (col <- headers) yield StructField(col, StringType, true) )

        val input = spark.read.schema(schema).csv(olympicFile)
        input.createOrReplaceTempView("olympic")

        // Number of athletes participated in each event
        val eventAthlete = spark.sql("SELECT SPORT, COUNT(DISTINCT ATHLETE) AS NUM_ATHLETE FROM OLYMPIC GROUP BY SPORT")
        eventAthlete.show(false)

        // Number of medals each country won
        val countryMedal = spark.sql("SELECT COUNTRY, YEAR, SUM(CAST(TOTAL AS INT)) AS TOTAL FROM OLYMPIC GROUP BY COUNTRY, YEAR ORDER BY COUNTRY, TOTAL DESC")
        countryMedal.show(false)

        // Top 10 athletes who won the highest gold medals
        val athleteGolds = spark.sql("SELECT ATHLETE, COUNTRY, SUM(CAST(GOLD AS INT)) AS GOLDS FROM OLYMPIC GROUP BY ATHLETE, COUNTRY ORDER BY GOLDS DESC")
        athleteGolds.show(10, false)

        // Number of athletes less than 20 years of age who won gold
        val lessThan20Years = spark.sql("SELECT COUNT(DISTINCT ATHLETE) AS LESS_THAN_20 FROM OLYMPIC WHERE CAST(AGE AS INT) < 20 AND CAST(GOLD AS INT) >= 1")
        lessThan20Years.show(false)

        // Youngest athlete who won gold in each category of sport
        val youngestAthlete = spark.sql("SELECT SPORT, ATHLETE, AGE, GOLD FROM OLYMPIC WHERE (SPORT, AGE) IN (SELECT SPORT, MIN(CAST(AGE AS INT)) FROM OLYMPIC GROUP BY SPORT) AND CAST(GOLD AS INT) >= 1 ORDER BY CAST(GOLD AS INT) DESC")
        youngestAthlete.show(false)

        spark.stop()
    }

}
