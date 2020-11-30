package org.training.spark.batch.sql

import org.apache.spark.sql.{SaveMode, SparkSession}

object Class5ParquetOrcFilesApp {

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName(getClass.getName)
      .master("local")
      .getOrCreate()

    val salesDf = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema","true").load(args(0))

    salesDf.coalesce(1).write
      .mode("overwrite")
      .option("compression", "none")
      .parquet("src/main/resources/output/parquet")

    val parquetSalesDF = spark.read.parquet("src/main/resources/output/parquet")


    parquetSalesDF.show

    parquetSalesDF.createOrReplaceTempView("sales")

    spark.sql("select itemId from sales").show()


    // Reading and Writing  Orc Files

    //spark.sql("select itemId from orc_sales").show()

    val orcPath = "src/main/resources/output/orc"
    parquetSalesDF
      .write
      .mode(SaveMode.Overwrite)
      .orc(orcPath)

    val orcSalesDF = spark.read.orc(orcPath)

    orcSalesDF.show
    orcSalesDF.createOrReplaceTempView("orc_sales")

  }

}
