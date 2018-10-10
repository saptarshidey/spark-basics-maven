package com.saptarshi.sparkscala.dataflair.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ StructType, StructField, StringType, LongType } 
import org.apache.spark.sql.functions._

object WorldBankIndicatorsDriver {

    def main(args: Array[String]): Unit = {
        require(args != null && args.length == 1, "World Bank Indicators Datafile required")
        val Array(worldBankFile) = args

        val spark = SparkSession.builder.appName("World Bank Indicators").getOrCreate()
        import spark.implicits._

        val headers = Array (
                "country",
                "effective_date",
                "trains",
                "cars",
                "mobile_phone_users",
                "internet_users",
                "mortality_under_5",
                "health_expenditure_per_capita",
                "health_expenditure_total",
                "population_total",
                "population_urban",
                "birth_rate",
                "life_expectancy_female",
                "life_expectancy_male",
                "life_expectancy_total",
                "population_percent_0_14",
                "population_percent_15_64",
                "population_percent_65_more",
                "gdp",
                "gdp_per_capita"
            )

        val schema = StructType( for (col <- headers) yield StructField(col, StringType, true) )
        val input = spark.read.schema(schema).csv(worldBankFile)

        // Top 2 most urban populous countries
        val countryUrbanPopulation = input.select($"country", regexp_replace($"population_urban", ",", "").cast(LongType) as "population_urban")
        val countryMaxUrbanPopulation = countryUrbanPopulation.groupBy("country").agg(max("population_urban") as "population_urban")
        countryMaxUrbanPopulation.orderBy($"population_urban".desc).show(2, false)

        // Top 2 highest population growth in the past decade
        val countryPopulation = input.select($"country", regexp_replace($"population_total", ",", "").cast(LongType) as "population_total")
        val countryMinMaxPopulation = countryPopulation.groupBy("country").agg(min("population_total") as "min_population", max("population_total") as "max_population")
        val countryPopulationGrowth = countryMinMaxPopulation.select($"country", ($"max_population" - $"min_population") / $"min_population" * 100.0 as "population_growth")
        countryPopulationGrowth.orderBy($"population_growth".desc).show(2, false)

        // Highest GDP growth from 2009 to 2010
        val countryGDP = input.select( $"country", coalesce(regexp_replace($"gdp", ",", ""), lit("0")).cast(LongType) as "gdp" ).where( split($"effective_date", "/")(2) isin ("2009", "2010") )
        val countryGDPMinMax = countryGDP.groupBy("country").agg( min("gdp") as "gdp_min", max("gdp") as "gdp_max" )
        val countryGDPGrowth = countryGDPMinMax.select( $"country", ($"gdp_max" - $"gdp_min") / $"gdp_min" * 100 as "gdp_growth" )
        countryGDPGrowth.orderBy($"gdp_growth".desc).show(false)

        spark.stop()
    }

}
