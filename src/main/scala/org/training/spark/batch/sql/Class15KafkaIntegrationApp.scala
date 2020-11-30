package org.training.spark.batch.sql

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object Class15KafkaIntegrationApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Class15KafkaIntegrationApp")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    /*
      subscribe  :	A comma-separated list of topics 	The topic list to subscribe.
      kafka.bootstrap.servers : 	A comma-separated list of host:port
                    The Kafka "bootstrap.servers" configuration.
     */

    val kafkaDF = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "sales_csv")
      .load()
      //.filter("timestamp>date_sub(current_date(),1)")

    kafkaDF.printSchema()

    val kafkaDF1 = kafkaDF
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition","offset")
      .withColumn("date",current_date())

    kafkaDF1
      .select(split(col("value"),",").as("data"))
      .selectExpr("data[0] transactionId", "data[1] customerId")
      .show()

    //Parsing csv message from kafka topic
    val kafkaDs = kafkaDF1.select("value").as[String]

    val salesDF = spark.read.csv(kafkaDs)
      .toDF("transactionId","customerId","itemId","amountPaid")

    salesDF//.show()

    //Produce data to kafka topic

    kafkaDF1.write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "spark_sales_out")
      //.save()

    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","cloudera")

    // read data from kafka topic and write to RDBMS
    kafkaDF1
      .write
      .mode(SaveMode.Overwrite)
      //.jdbc("jdbc:mysql://localhost:3306/ecommerce","kafka_sales",props)


  }
}
