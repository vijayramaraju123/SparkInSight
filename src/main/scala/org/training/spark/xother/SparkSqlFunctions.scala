package org.training.spark.xother

import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlFunctions {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("SparkSqlFunctions")

    val sc = new SparkContext(conf)
    //val sqlCtx = new SQLContext(sc)
    val sqlCtx = new HiveContext(sc)

    val optionsObj = Map("header" -> "true",
      "inferSchema" -> "true",
      "delimiter" -> ",")

    val salesDF = sqlCtx.read.format("csv")
      .options(optionsObj)
      .load(args(0))

    import sqlCtx.implicits._

    val salesDF1 = salesDF
      .select($"transactionId",$"customerId",$"itemId",$"amountPaid",($"amountPaid"*0.1).as("discount"))

    val salesDF2 = salesDF1.withColumn("finalAmount",col("amountPaid").minus($"discount"))
      .withColumn("CreatedDateTime",current_timestamp())
      .withColumn("CreatedBy",lit("ETLAdmin"))
      .withColumn("DateString",col("CreatedDateTime").cast(StringType))
      .withColumnRenamed("DateString","Date")
      .withColumnRenamed("discount","newDiscount")
      .withColumn("Flag",when(col("amountPaid")>2500,"Ext" +
        "raPrime")
                                  .when(col("amountPaid").between(1000,2500),"Prime")
                                  .otherwise("NonPrime"))
      //.show

    val aggDF = salesDF1.groupBy("customerId").agg(sum("amountPaid").alias("sum"),
      avg("amountPaid").alias("avg"),
      count("amountPaid").alias("count"),
      max("amountPaid").alias("maxAmount"),
      countDistinct("amountPaid").alias("UniqueCount"),
      collect_list("amountPaid").alias("amountList")
      )

    aggDF//.printSchema()
    aggDF//.show()


    //salesDF2.show()

    salesDF2.select("customerId","Flag","amountPaid")//.show()


    salesDF.drop("itemId").dropDuplicates()
      .cache()
      .persist(StorageLevel.MEMORY_AND_DISK)

    salesDF.except(salesDF.limit(10)).show()


  }

}
