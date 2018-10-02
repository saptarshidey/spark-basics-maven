package com.saptarshi.sparkscala.examples

import org.apache.spark.sql.SparkSession

object JoinOperation {

    def main(args: Array[String]): Unit = {
        require(args != null && args.length == 2, "Employee and Employee_Details Files required")
        val Array(empFile, empDetailFile) = args

        val spark = SparkSession.builder.appName("Join Operation").getOrCreate()
        import spark.implicits._

        val cols = Map(
                "employee" -> Seq("employee_id", "name"),
                "employee_details" -> Seq("employee_id", "designation", "gender")
            )
        val employees = spark.read.csv(empFile).toDF(cols.get("employee").get: _*)
        val employeeDetails = spark.read.csv(empDetailFile).toDF(cols.get("employee_details").get: _*)

        val result = employees.join(employeeDetails, "employee_id")
        result.collect.foreach(println)

        spark.stop()
    }

}
