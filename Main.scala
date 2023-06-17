package com.sparkbyexamples.spark.rdd

import org.apache.commons.lang3.ObjectUtils.median
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra.CassandraSQLRow.empty.getAs
import org.apache.spark.sql.execution.command.ClearCacheCommand.stats
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("Nagraj")
      .getOrCreate()

    println("read csv files base on wildcard character")
    val data = spark.read.option("header", "true").csv("C:\\Users\\91703\\IdeaProjects\\projectR.scala\\input\\data.csv")
    data.show()
    //Give marketing success rate. (No. of people subscribed / total no. of entries) (Spark SQL)
    data.createOrReplaceTempView("mytable")
    val successDF = spark.sql("SELECT count(*) AS successCount FROM mytable WHERE poutcome = 'success'")
    val failureDF = spark.sql("SELECT count(*) AS successCount FROM mytable WHERE poutcome = 'failure'")
    successDF.show()
    val numDF = spark.sql("SELECT count(*) AS totalCount FROM mytable")

    numDF.show()
    val successCount = successDF.first().getLong(0)
    val failureRate = failureDF.first().getLong(0)
    println(successCount)
    val totalCount = numDF.first().getLong(0)
    println(totalCount)
    val MarketingSuccessRate = successCount.toDouble / totalCount
    //Give marketing failure rate
    val marketingFailureRate = failureRate.toDouble / totalCount

    println("Marketing Success Rate: " + MarketingSuccessRate)
    println("Marketing Failure Rate: " + marketingFailureRate)

    //3  Maximum, Mean, and Minimum age of average targeted customer
    val age = spark.sql("SELECT count(*)  FROM mytable")
    println("avg: " + data.select(avg("age")).collect()(0)(0))
    println("min: " + data.select(min("age")).collect()(0)(0))
    println("max: " + data.select(max("age")).collect()(0)(0))

    // 4. Check quality of customers by checking average balance , median balance of customers

    println("avgerage balance : " + data.select(avg("balance")).collect()(0)(0))

    val medianValue = data.select(median("balance")).collect()(0)(0)
    println(s"The median price is: $medianValue")

    // 5. Check if age matters in marketing subscription for deposit

    val  agematters = spark.sql("select age, count(*) as number from  mytable where y='yes' group by age order by number desc")
      .show()

    //6.Check if marital status mattered for a subscription to deposit.
    val customers_by_marital = spark.sql("select marital, count(*) as number from mytable where y='yes' group by marital order by number desc")
      .show()

    //7 Check if age and marital status together mattered for a subscription to deposit scheme.
    val customers_by_agemarital = spark.sql("select age, marital, count(*) as number from  mytable where y='yes' group by age, marital order by number desc")
      .show()

    
  }
}


