package org.training.spark.batch.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.training.spark.batch.utils.FunctionUtils

object Class9UdfExamplesApp {

  def getDiscount(amount:Double):Double = {
    if(amount<2000) amount*0.1
    else amount*0.2
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Class11RdbmsIntegrationApp")
      .master("local")
      .getOrCreate()

    val salesDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/datasets/sales.csv")

    salesDF.createOrReplaceTempView("sales")

    //Registering UDF with Spark Context

    //Approach 1
    val discountUdf = udf(FunctionUtils.getDiscount _)
    val discUdf = udf((amount:Double) => if(amount>2000) amount*0.2 else amount*0.1)

    salesDF.withColumn("discount1", discountUdf(col("amountPaid")))
      .withColumn("discount2",discUdf(col("amountPaid")))
      //.show()

    //Approach 2 : this approach will register function for both DSL and SQL queries.

    val discount = spark.udf.register("discount", getDiscount _)

    spark.sql("select *, discount(amountPaid) discount from sales").show()
    salesDF.withColumn("discount",discount(col("amountPaid"))).show()

  }


}
