package org.training.spark.batch.sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._


object Class14CassandraIntegrationApp {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Class14CassandraIntegrationApp")
      .master("local")
      .getOrCreate()

    spark.conf.set("spark.cassandra.connection.host", "localhost")
    //sparkConf.set("spark.cassandra.connection.keep_alive_ms", "40000")

    val cassandraOptions = Map("table" -> "sales", "keyspace" -> "spark_test")

    val salesDF = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(cassandraOptions)
      .load()

    salesDF.show()

    val csvSalesDf = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(args(0))
      .toDF("tid","custid","itemid","price")

    csvSalesDf.printSchema()

    val cassandraOptions1 = Map( "table" -> "sales", "keyspace" -> "spark_test", "confirm.truncate" -> "true")

    //Write or overwrite
    csvSalesDf.write
      .format("org.apache.spark.sql.cassandra")
      .options(cassandraOptions1)
      .mode(SaveMode.Overwrite)
      //.save()

    //Upsert data into cassandra
    salesDF
      .withColumn("price",col("price")*3)
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(cassandraOptions1)
      .mode(SaveMode.Append)
      .save()
  }
}
