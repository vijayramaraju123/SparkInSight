package org.training.spark.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object Class2KafkaStreamApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getName)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val streamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe","sales_json")
      .load()

    // parsing text data stream from kafka

    val totalValueDF = streamDF
      .select(split(col("value"),",").as("value"))
      .selectExpr("value[1] customerId","value[3] amount")
      .groupBy("customerId")
      .agg(sum("amount").as("total"))

//    val query = totalValueDF
//      .writeStream
//      .format("console")
//      .outputMode("update") //append,complete,update
//      .trigger(Trigger.ProcessingTime(10000))
//      .start()

    //query.awaitTermination()

    // parsing json data stream from kafka topic

    val schema = new StructType()
      .add("transactionId",StringType)
      .add("customerId",IntegerType)
      //.add("itemId",IntegerType)
      .add("amountPaid",DoubleType)

    val salesDF = streamDF.selectExpr("cast(value as string) sales_json")
      .select(from_json(col("sales_json"),schema).as("jsonData"))
      .select("jsonData.*").withColumn("date",current_date().cast(StringType))

//    val jsonQuery = salesDF
//      .writeStream
//      .format("console")
//      .start()

    val colums = salesDF.columns.map(col)

    val jsonQuery = salesDF.select(to_json(struct(colums:_*)).as("value").cast(BinaryType))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("checkpointLocation","src/main/resources/checkpoint/chk3")
      .option("topic","sales_out")
      .start()

    //salesDF.writeStream.foreach(x => x)

    jsonQuery.awaitTermination()


  }
}
