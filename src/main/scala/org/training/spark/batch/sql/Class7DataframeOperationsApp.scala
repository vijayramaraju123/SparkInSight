package org.training.spark.batch.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object Class7DataframeOperationsApp extends App {

  val spark = SparkSession.builder()
    .appName(getClass.getName)
    .master("local")
    .getOrCreate()

  val salesDF = spark.read.format("csv")
        .option("header","true")
        .option("inferSchema","true")
        .load(args(0))

  salesDF.select("customerId","amountPaid")
    //.show() // this is called DSL query

  salesDF.createOrReplaceTempView("sales")
  val sqlSalesDf = spark.sql("select customerId, amountPaid*2 from sales") //SQL approach
  //sqlSalesDf.show()

  //Filtering rows from dataframe

  // DSL Query
//  salesDF.where("amountPaid>1000").show()
//  salesDF.filter("amountPaid>1000").show()
//  // SQL Query
//  spark.sql("select * from sales where amountPaid>1000").show()
//
//  salesDF.where(col("amountPaid").gt(1000)).show()
//  salesDF.filter(col("amountPaid") > 1000).show()

  //Adding columns from Dataframe

  salesDF.selectExpr("transactionId","customerId","itemId","amountPaid","amountPaid*0.2 discount")
    //.show()

  salesDF.select(col("transactionId"),col("customerId"),col("itemId"),col("amountPaid"),(col("amountPaid")*0.2).alias("discount"))
    //.show()

  salesDF.withColumn("discount", col("amountPaid")*0.2)
    .withColumn("total",col("amountPaid")-col("discount"))
    //.show()

  //Renaming columns

  salesDF.withColumnRenamed("transactionId","orderId")//.show()

  //Dropping Columns from Dataframe

  salesDF.drop("transactionId","itemId")//.show()

  //Merging two dataframes

  val newSalesDF = salesDF.filter("amountPaid>2000")

  salesDF.union(newSalesDF)//.show(50)
  //Returns Common rows
  salesDF.intersect(newSalesDF)//.show()
  //like minus clouse in SQL
  salesDF.except(newSalesDF)//.show()

  //Remove duplicate rows
  salesDF.distinct()//.show()
  salesDF.dropDuplicates()//.show()
  salesDF.dropDuplicates("itemId","amountPaid")//.show()

  //Caching Dataframes

  salesDF.cache()
  salesDF.persist(StorageLevel.MEMORY_ONLY)

  //Increasing and decreasing num of partitions

  salesDF.coalesce(1)
  //this can increase or decrease the num of partitions
  salesDF.repartition(2)
  salesDF.repartition(2,col("customerId"))

  //Dataframe Aggregate Functions

  salesDF.agg(sum("amountPaid").as("total"))//.show()
  spark.sql("select sum(amountPaid) as total from sales")//.show()

  salesDF.groupBy("customerId").sum("amountPaid")//.show()

  salesDF.groupBy("customerId").agg(sum("amountPaid").as("total"),
      count("amountPaid").as("count"),
      min("amountPaid").as("min"),
      max("amountPaid").alias("max"),
      avg("amountPaid").alias("avg"),
      collect_list("itemId").alias("items"),
      collect_set("itemId").alias("uniqueItems")
    )//.show(false)

  salesDF.groupBy("customerId","itemId").agg(sum("amountPaid").as("total"),
    count("amountPaid").alias("count"),
    min("amountPaid").alias("min"),
    max("amountPaid").alias("max"),
    avg("amountPaid").alias("avg"),
    collect_list("itemId").alias("items"),
    collect_set("itemId").alias("uniqueItems")
  )//.show(false)

  // Ordering dataframes
  salesDF.orderBy("amountPaid")//.show()
  salesDF.orderBy(col("amountPaid").desc)//.show()

  //Pivot

  //val pivotDf = salesDF.groupBy("customerId").pivot("itemId").agg(sum("amountPaid"))
  val pivotDf = salesDF.groupBy("customerId")
    .pivot("itemId", List(1,3))
    .agg(sum("amountPaid").as("total"),count("amountPaid").as("count"))

  pivotDf//.show()

  //Replacing null values in DF

  pivotDf.na.fill(0)//.show()
  pivotDf.na.fill(0,Array("1_total","3_total"))//.show()

  //Other usful methods

  println("Dataframe columns : "+salesDF.columns.toList)
  println("Dataframe dtypes : "+salesDF.dtypes.toList)
  println("Dataframe schema : "+salesDF.schema.toList)

  // Action Methods
  salesDF.collect.foreach(println)
  salesDF.take(5).foreach(println)
  println("count : "+salesDF.count())

  salesDF.printSchema()

//  val columns = salesDF.columns.filter(x => x.contains("Id"))
//
//  salesDF.drop(columns:_*).show()

}
