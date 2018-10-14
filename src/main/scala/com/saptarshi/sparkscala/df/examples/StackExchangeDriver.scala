package com.saptarshi.sparkscala.df.examples

import scala.xml.XML
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._

object StackExchangeDriver {

    def main(args: Array[String]): Unit = {
        require(args != null && args.length == 1, "Stack Exchange Datafile required")
        val Array(stackExchangeFile) = args

        val spark = SparkSession.builder.appName("Stack Exchange Data Analysis").getOrCreate()
        import spark.implicits._

        val headers = Array (
            "Id",
            "PostTypeId",
            "AcceptedAnswerId",
            "ParentId",
            "CreationDate",
            "Score",
            "ViewCount",
            "Body",
            "OwnerUserId",
            "OwnerDisplayName",
            "LastEditorUserId",
            "LastEditorDisplayName",
            "LastEditDate",
            "LastActivityDate",
            "Title",
            "Tags",
            "AnswerCount",
            "CommentCount",
            "FavoriteCount",
            "ClosedDate",
            "CommunityOwnedDate"
        )

        val input = spark.read.text(stackExchangeFile)

        /* Databricks provides a spark-xml package to read xml files, but the library does not support files that contain
         * rows with self closing xml tags. Also it does not have a consistent behavior for large files. In this example,
         * we are loading the data as a Text file (each line is a xml Element) and then using scala.xml package to parse
         * the data and create a DataFrame */

        val xmlData = input.filter(_.getString(0).trim.startsWith("<row"))
                           .map(row => {
                                    val xml = XML.loadString(row.getString(0))
                                    (
                                        xml.attribute("Id").getOrElse("").toString,
                                        xml.attribute("PostTypeId").getOrElse("").toString,
                                        xml.attribute("AcceptedAnswerId").getOrElse("").toString,
                                        xml.attribute("ParentId").getOrElse("").toString,
                                        xml.attribute("CreationDate").getOrElse("").toString,
                                        xml.attribute("Score").getOrElse("").toString,
                                        xml.attribute("ViewCount").getOrElse("").toString,
                                        xml.attribute("Body").getOrElse("").toString,
                                        xml.attribute("OwnerUserId").getOrElse("").toString,
                                        xml.attribute("OwnerDisplayName").getOrElse("").toString,
                                        xml.attribute("LastEditorUserId").getOrElse("").toString,
                                        xml.attribute("LastEditorDisplayName").getOrElse("").toString,
                                        xml.attribute("LastEditDate").getOrElse("").toString,
                                        xml.attribute("LastActivityDate").getOrElse("").toString,
                                        xml.attribute("Title").getOrElse("").toString,
                                        xml.attribute("Tags").getOrElse("").toString,
                                        xml.attribute("AnswerCount").getOrElse("").toString,
                                        xml.attribute("CommentCount").getOrElse("").toString,
                                        xml.attribute("FavoriteCount").getOrElse("").toString,
                                        xml.attribute("ClosedDate").getOrElse("").toString,
                                        xml.attribute("CommunityOwnedDate").getOrElse("").toString
                                    )
                                }
                            ).toDF(headers: _*)

        val questions = xmlData.filter($"PostTypeId" === "1")
 
        // Questions asked per month
        questions.groupBy( split($"CreationDate", "-")(1) as "month" ).agg( count("Id") as "question_count" ).orderBy($"question_count".desc).show(false)

        // Questions containing specific words in their title
        val questionWithWords = questions.filter( $"Title".contains("data") || $"Title".contains("science") || $"Title".contains("hadoop") || $"Title".contains("spark") ).count
        println(s"Number of Questions containing 'data', 'science', 'hadoop', 'spark' = ${ questionWithWords }")

        // Top 10 highest viewed questions
        questions.orderBy( $"ViewCount".cast(IntegerType).desc ).show(10)

        // Questions with more than 2 answers
        val questionsWithMoreThan2Ans = questions.filter( $"AnswerCount".cast(IntegerType) > 2 ).count
        println(s"Number of Questions with more than 2 Answers = ${ questionsWithMoreThan2Ans }")

        // Questions that are active for the last 6 months
        val questionsActiveFor6Months = questions.filter( datediff(to_date($"LastActivityDate", "yyyy-MM-dd'T'HH:mm:ss.SSS"), to_date($"CreationDate", "yyyy-MM-dd'T'HH:mm:ss.SSS")) >= 180).count
        println(s"Number of Questions active for last 6 Months = ${ questionsActiveFor6Months }")

        // List of all Tags along with their count
        questions.select( explode(split(regexp_replace(regexp_replace($"Tags", "&lt", ""), "&gt", "" ), ";")) as "Tags" ).groupBy("Tags").agg(count("Tags") as "Count").orderBy($"Count".desc).show(false)

        // Average time for a post to get a correct answer
        val questionsWithAcceptedAnswer = questions.filter(length($"AcceptedAnswerId") =!= 0).alias("q")
        val answers = xmlData.filter($"PostTypeId" === "2").alias("a")
        val answerTime = questionsWithAcceptedAnswer.join(answers, $"q.AcceptedAnswerId" === $"a.Id").select( $"q.Id", ( unix_timestamp($"a.CreationDate", "yyyy-MM-dd'T'HH:mm:ss.SSS") - unix_timestamp($"q.CreationDate", "yyyy-MM-dd'T'HH:mm:ss.SSS") ) / 3600.0 as "hours" )

        val ansCount = answerTime.count
        val totHours = answerTime.map(_.getDouble(1)).reduce(_ + _)

        val avgTimeForCorrectAnswer = totHours / ansCount
        println(s"Average Time for a correct answer = ${ avgTimeForCorrectAnswer } hours")

        spark.stop()
    }

}
