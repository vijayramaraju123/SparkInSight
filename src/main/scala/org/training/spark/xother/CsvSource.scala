package org.training.spark.xother

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object CsvSource extends App{

  val conf = new SparkConf().setMaster(args(0)).setAppName("CsvSource")

  val sc = new SparkContext(conf)

  val sqlCtx = new SQLContext(sc)

  val salesDF = sqlCtx.read.format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load(args(1))

  salesDF.printSchema()
  salesDF.show

  val optionsObj = Map("header" -> "true",
    "inferSchema" -> "true",
    "delimiter" -> "|")

  val pipeSalesDF = sqlCtx.read.format("csv")
    .options(optionsObj)
    .load(args(2))

  pipeSalesDF.printSchema()
  pipeSalesDF.show()

  pipeSalesDF

  val jsonSalesDF = sqlCtx.read.format("json")
    //.options(optionsObj)
    .load(args(3))

  jsonSalesDF.show()


  jsonSalesDF.write.format("csv").option("header","true")
    .save("src/main/resources/csvOutput1")

}
