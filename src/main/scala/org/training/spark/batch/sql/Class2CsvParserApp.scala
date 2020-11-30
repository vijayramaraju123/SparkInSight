package org.training.spark.batch.sql

import org.apache.spark.sql.{SaveMode, SparkSession}

object Class2CsvParserApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(getClass.getName)
      .master("local")
      .getOrCreate()

    // Approach 1
    val salesDF2 = spark.read.format("csv")
      .option("header", "true")
      //.option("inferSchema", "true")
      .schema("transactionId long,customerId int,itemId int,amountPaid double")
      .load(args(0))

    salesDF2.printSchema()

    //salesDF2.show()

    //Approach 2 using Spark 2.X libraries

    val salesDF3 = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(0))

    salesDF3.printSchema()

    salesDF3
      .select("transactionId","amountPaid")
      .show()

    salesDF3
      .selectExpr("transactionId as id", "amountPaid*2 amount")
      .show()

    val outputDf = salesDF3
      .selectExpr("transactionId orderID","customerId","amountPaid*5 transaction_amount")

    outputDf.coalesce(1)
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .option("header","true")
      .option("delimiter","|")
      .save("src/main/resources/output/csv/outputDf")

    outputDf
      .write
      .option("header","true")
      .csv("src/main/resources/output/csv/outputDf1")

  }
}
