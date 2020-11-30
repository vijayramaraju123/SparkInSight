package org.training.spark.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger


object Class1SocketStreamApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
                .builder()
                .master("local[*]")
                .appName(getClass.getName)
                .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val streamDF = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",9999)
      .load()

//    val query = streamDF.writeStream
//      .format("console")
//      .start()

    val totalValueDF = streamDF
      .select(split(col("value"),",").as("value"))
      .selectExpr("value[1] customerId","value[3] amount")
      .groupBy("customerId")
      .agg(sum("amount").as("total"),
        avg("amount").as("avg"))

    /*writeStream options :
      outputMode :
        append :  only the new rows in the streaming DataFrame/Dataset will be written to the sink
        complete: all the rows in the streaming DataFrame/Dataset will be written to the sink every time these is some updates
        update:   only the rows that were updated in the streaming DataFrame/Dataset will be written to the sink every time there are some updates.
                  If the query doesn't contain aggregations, it will be equivalent to `append` mode.
      Trigger : A trigger policy that runs a query periodically based on an interval in processing time.
      format : console / kafka / parquet or orc or json or csv or some custom formats using foreach
     */

//    val query = totalValueDF
//      .writeStream
//      .format("console")
//      .outputMode("update") //append,complete,update
//      .trigger(Trigger.ProcessingTime(10000))
//      .start()

    //Write stream data to a file system

    val query = streamDF.writeStream
      .format("csv")
      .option("path", "src/main/resources/output/stream2")
      .option("checkpointLocation","src/main/resources/checkpoint/chk2")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(10000))
      .start()

//    val query1 = totalValueDF
//      .writeStream
//      .format("console")
//      .outputMode("update") //append,complete,update
//      .trigger(Trigger.ProcessingTime(10000))
//      .start()

    query.awaitTermination()
    //query1.awaitTermination()

  }
}
