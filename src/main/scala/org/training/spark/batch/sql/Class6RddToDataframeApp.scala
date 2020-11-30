package org.training.spark.batch.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.training.spark.batch.utils.{Sales, SalesRecord}

/**
 * Creating Dataframe from RDD case classes
 */

object Class6RddToDataframeApp {


  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName(getClass.getName)
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val rawSalesRdd = spark.sparkContext.textFile(args(0))

    // Creating Dataframe using schema and RDD

    val salesRowRdd = rawSalesRdd
      .filter(line => !(line.startsWith("transactionId") || line.startsWith("Count")))
      .map(_.split("\\|"))
      .map(p => Row(p(0).trim, p(1).trim.toInt, p(2).trim.toInt, p(3).toDouble))

    val schema =
      StructType(
        Array(StructField("transId", StringType, true),
          StructField("customerId", IntegerType, true),
          StructField("itemId", IntegerType, true),
          StructField("amountPaid", DoubleType, true))
      )

    val schemaDF = spark.createDataFrame(salesRowRdd,schema)

//    schemaDF.printSchema()
//    schemaDF.show()

    //Creating Dataframe from RDD case class

    val salesRDD = rawSalesRdd
      .filter(line => !(line.startsWith("transactionId") || line.startsWith("Count")))
      .map(_.split("\\|"))
      .map(p => SalesRecord(p(0).trim, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble))

    val salesDF = spark.createDataFrame(salesRDD)

//    salesDF.printSchema()
//    salesDF.show()

    // Creating Dataframe from RDD case class using toDF
    // We need to import implicits from spark / sqlContext object to access toDF method

    import spark.implicits._

    val salesRdd1 = rawSalesRdd.filter(line => !(line.startsWith("transactionId") || line.startsWith("Count")))
      .map(_.split("\\|"))
      .map(p => Sales(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble))

    val salesDF1 = salesRdd1.toDF("ID","customerId","itemId","amountPaid")
//    salesDF1.printSchema()
//    salesDF1.show()

    // Creating Dataframe from RDD tuple elements

    val tupleRDD = rawSalesRdd
      .filter(line => !(line.startsWith("transactionId") || line.startsWith("Count")))
      .map(_.split("\\|"))
      .map(p => (p(0).trim, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble))

//   tupleRDD.toDF().show()
//
//    tupleRDD.toDF("transactionId","customerId","itemId","amountPaid").show()

    // Converting Dataframe into RDD

    val salesRdd = salesDF.rdd

    salesRdd.foreach(println)


  }
}
