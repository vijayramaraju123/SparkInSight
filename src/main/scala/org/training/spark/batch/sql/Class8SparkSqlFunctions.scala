package org.training.spark.batch.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}


object Class8SparkSqlFunctions {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(getClass.getName)
      .master("local")
      .getOrCreate()

    val salesDF = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("C:\\Users\\Incredible\\Documents\\run\\PrasadSirProject\\sales.csv")

    // Basic Functions

    salesDF.select(col("itemId").alias("item"))//.show()

    val finalSalesDf = salesDF
      .withColumn("creadtedBy",lit("spark_sql"))
      .withColumn("createDate",current_date())
      .withColumn("createDateTime",current_timestamp())

    finalSalesDf//.show()

    salesDF.createOrReplaceTempView("sales")

    spark.sql("select *, 'spark_sql' creadtedBy, current_date as createDate, current_timestamp as createDateTime from sales")
      //.show()

    //Concat two or more columns
    salesDF.select(concat(col("customerId"),col("amountPaid")))
      //.show()

    salesDF.select(concat_ws("-",col("customerId"),lit("abc"), col("amountPaid")).alias("concat"))
      //.show()

    salesDF.selectExpr("concat_ws('-',customerId,amountPaid) as concat", "current_date as createDate")
      //.show()

    //struct and array functions

    val structDf = salesDF.select(col("transactionId"),col("customerId"),struct("itemId","amountPaid").alias("itemStruct"))
      //.toJSON

    salesDF.printSchema()
    structDf.printSchema()
    structDf.toJSON//.show(false)

    salesDF.select(col("transactionId"),col("customerId"),array("itemId","amountPaid").alias("itemArray"))
      .toJSON
      //.show(false)

    salesDF.selectExpr("transactionId", "customerId", "array(itemId,amountPaid) itemArray")
      //.toJSON
      //.show(false)


    //split, substring

    finalSalesDf.select(split(col("creadtedBy"),"_")(0).as("created"),substring(col("creadtedBy"),0,5).as("substr"))
      //.show()

    //case statements

    salesDF.withColumn("flag", when(col("amountPaid") > 4000,2).when(col("amountPaid") < 4000 && col("amountPaid") > 2000,1).otherwise(0))
      .withColumn("flag1", expr("case when amountPaid>4000 then 2 when amountPaid>2000 then 1 else 0 end"))
      .withColumn("discount", expr("amountPaid*0.2"))
      //.show()

    //date functions

    salesDF.withColumn("date_str",lit("2020/05/01"))
      .select(to_date(col("date_str")).as("date1"), to_date(col("date_str"),"yyyy/MM/dd").as("date"))
      .withColumn("date1",date_format(col("date"),"yyyyMMdd"))
      .withColumn("date2",date_format(col("date"),"yyyyMMdd hh:mm:ss"))
      .withColumn("nextDate",date_add(col("date"),5))
      .withColumn("oldDate",date_sub(col("date"),5))
      .withColumn("addMonth",add_months(col("date"),3))
      .withColumn("year",year(col("date")))
      .withColumn("weekOfYear",weekofyear(col("date")))
      .show()

    //Json string functions

    val schema = new StructType().add("amountPaid",DoubleType)
    val schema1 = StructType(Array(StructField("itemId",DoubleType,true)))

    salesDF.withColumn("json",to_json(struct("itemId","amountPaid")))
     .withColumn("from_json", from_json(col("json"),schema))
     //.show(false)

    //agg function was explained in previous class

    salesDF.groupBy("customerId").agg(sum("amountPaid").as("total"),
      count("amountPaid").as("count"),
      min("amountPaid").as("min"),
      max("amountPaid").alias("max"),
      avg("amountPaid").alias("avg"),
      collect_list("itemId").alias("items"),
      collect_set("itemId").alias("uniqueItems")
    )//.show(false)

    //Window function or analytical functions

    val rowWindow = Window.orderBy("transactionId")

    salesDF.withColumn("rowNumber", row_number().over(Window.orderBy("transactionId")))
      .withColumn("rowNumber1", expr("row_number() over(ORDER BY transactionId, customerId)"))
      .withColumn("rank", rank().over((Window.partitionBy("customerId").orderBy(desc("amountPaid")))))
      .withColumn("dense_rank",dense_rank().over((Window.partitionBy("customerId").orderBy(col("amountPaid").desc))))
      .where("dense_rank=2")
      .show()

    //casting column data types

    import spark.implicits._

    salesDF.select(col("amountPaid").cast(IntegerType))
      //.show()

    salesDF.selectExpr("cast(amountPaid as int) amountPaid")
      //.show


  }

}
