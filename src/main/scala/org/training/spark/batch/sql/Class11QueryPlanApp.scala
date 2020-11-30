package org.training.spark.batch.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Class11QueryPlanApp extends App {

  val ss = SparkSession
    .builder()
    .appName("Class11QueryPlanApp")
    .master("local")
    .getOrCreate()

  val salesDf = ss.read
    .format("org.apache.spark.sql.json")
    .option("header", "true")
    .load("src/main/resources/datasets/sales.json")

  val customerDf = ss.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/datasets/customers.csv")

  customerDf.printSchema()
  salesDf.printSchema()

  val quareyPlanDf = customerDf
    .withColumnRenamed("customerId", "customerIdentity")
    .join(salesDf, col("customerIdentity") === col("customerId"))
    .filter("amountPaid > 100.0")
    .filter("itemId in (1,3)")

  quareyPlanDf.show()

  println("Parsed logical plan : ")
  println(quareyPlanDf.queryExecution.logical)

  println("analyzed logical plan: ")
  println(quareyPlanDf.queryExecution.analyzed)

  println("optimised logical plan: ")
  println(quareyPlanDf.queryExecution.optimizedPlan)

  println("Physical Plan: ")
  quareyPlanDf.explain()

  Thread.sleep(500000)

}
