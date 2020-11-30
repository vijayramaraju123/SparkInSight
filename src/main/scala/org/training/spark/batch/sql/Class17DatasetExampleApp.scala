package org.training.spark.batch.sql

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.training.spark.batch.utils.{Customers, Sales}

case class SalesAmount(transactionId:Int,amountPaid:Double)

object Class17DatasetExampleApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Class17DatasetExampleApp")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    //dataset wordcount

    val dataDs = spark.read.textFile("src/main/resources/datasets/data.txt")

    dataDs//.show()

    //val dataRdd = spark.sparkContext.textFile("src/main/resources/datasets/data.txt")
    //val dataRdd = spark.read.textFile("src/main/resources/datasets/data.txt").rdd

    val words = dataDs.flatMap(rec => rec.split(" "))

//    val wordCountDs = words
//      .groupByKey(x => x.toLowerCase)
//      .count()

    val wordCountDs = words
      .groupByKey(x => x.toLowerCase)
      .agg(count("*"))

    wordCountDs
      .toDF("word","count")
      //.show()


    //Working with sales data using dataset

    val salesDf = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("src/main/resources/datasets/sales.csv")

    val salesDs = salesDf.as[Sales]

    val salesDs1 = spark.read.textFile("src/main/resources/datasets/sales.csv")
      .map(x => x.split(","))
      .map(x => Sales(x(0).toInt,x(1).toInt,x(2).toInt,x(3).toDouble))


    salesDf.select("itemId")

    salesDs.map(rec => rec.itemId)
    salesDs.select("itemId")

    salesDs.filter(rec => rec.amountPaid > 500) // compile time error
    //salesDs.filter("amountPaid1 > 500") // no compile time error

    salesDs.createOrReplaceTempView("sales_ds")

    spark.sql("select transactionId,amountPaid from sales_ds where amountPaid>500")
      //.show()

    // Joins using Datasets

    val customerDs = spark.read.option("header","true")
      .option("inferSchema","true")
      .csv("src/main/resources/datasets/customers.csv")
      .as[Customers]

    val finalDf = salesDs.join(customerDs,"customerId")

    finalDf//.show()


    val salesAmountDs  = salesDs
      .select("transactionId","amountPaid")
      .as[SalesAmount]

    salesAmountDs//.show()

    val discSalesDs = salesDs
      .map(rec => {
        val discount = rec.amountPaid * 0.2
        (rec.transactionId,rec.customerId,rec.amountPaid,discount)
      })

    discSalesDs.show()
    salesDs.withColumn("discount", col("amountPaid")*.2).show()

    val discDf = discSalesDs
      .toDF("transactionId","customerId","amountPaid","discount")

    discDf.show()

    salesDs.toDF().show()

  }


  def getSalesDf(path: String, spark: SparkSession): Dataset[Row] = {
    spark.read.csv(path)
  }

}
