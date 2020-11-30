package org.training.spark.batch.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Class16AwsS3IntegrationApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Class16AwsS3IntegrationApp")
      .master("local")
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIAJJW3BDKG3SNONRCA")
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "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")

    val salesDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("s3n://prasad-test1/data/sales.csv")
      .withColumn("date",current_date())

    salesDF.show()

    salesDF.write
      .option("header","true")
      .csv("s3n://prasad-test1/data/sales_out")

  }
}
