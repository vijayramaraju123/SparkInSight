package org.training.spark.batch.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Class10SqlJoinsApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(getClass.getName)
      .master("local")
      .getOrCreate()

    val salesDf = spark.read
      .format("json")
      .option("header", "true")
      .load("src/main/resources/datasets/sales.json")

    val customerDf = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/datasets/customers.csv")

    /*Approach1:
      This approach works fine when joining two tables with same key column name.
      If you have different column names for key columns then we should go for Approach2
    */

    salesDf.printSchema()
    customerDf.printSchema()

    customerDf.join(salesDf, "customerId")
      .select("transactionId","customerId","name","amountPaid")
      //.show()

    /*
      Approach2: When we have different key column names
      Defualt join type --> Inner
    */

    customerDf.withColumnRenamed("customerId", "customerIdentity").alias("A")
      .join(salesDf.alias("B"), col("A.customerIdentity") === col("B.customerId"))
      .selectExpr("A.name","B.*")
      //.show()

//    customerDf.alias("A")//.withColumnRenamed("customerId", "customerIdentity")
//      .join(salesDf.alias("B"), col("A.customerId") === col("B.customerId") && col("A.name") === col("B.name"))
//      .selectExpr("A.name","B.*")
//      .show()

    /*Approach3 : When we have multiple key columns with same name*/
    println("Inner Join Output:")

    customerDf
      .join(salesDf, Seq("customerId"))
      .orderBy("customerId")
      //.show()

    /*Supported joins in spark sql*/

    /*Left Outer:
        join returns all rows from the left dataset regardless of match found on the right dataset.
        when join expression doesn’t match, it assigns null for that record, drops records from right where match not found.
        Alternatives keywords: left, leftouter
    */
    println("Left Outer Join Output:")
    customerDf.join(salesDf, Seq("customerId"), "left")
    //.show()

    /*Right Outer:
        join is opposite of left join, here it returns all rows from the right dataset regardless of math found on the left dataset.
        when join expression doesn’t match, it assigns null for that record and drops records from left where match not found.
        alternatives keywords: right, rightouter
    */
    println("Right Outer Join Output:")
    customerDf.join(salesDf, Seq("customerId"), "right")
    .show()

    /*full Outer:
        join returns all rows from both datasets,
        when join expression doesn’t match it returns null on respective record columns.
        alternatives keywords: outer, full, fullouter
    */
    println("Full Outer Join Output:")
    customerDf.join(salesDf, Seq("customerId"), "full")
    //.show()


    /*Left Semi Join:
        leftsemi join is similar to inner join difference being leftsemi join returns all columns from the left dataset and ignores all columns from the right dataset.
        In other words, this join returns columns from only left dataset for the records match in the right dataset on join expression.
        records not matched on join expression are ignored from both left and right datasets.
    */
    println("Left Semi Join Output:")
    customerDf.join(salesDf, Seq("customerId"), "leftsemi")
    //.show()

    println("Left Semi Anti Output:")
    /*Left Anti:
        join does the exact opposite of the leftsemi, leftanti join returns only columns from the left dataset for non-matched records.
    */
    customerDf.join(salesDf, Seq("customerId"), "leftanti")
    //.show()


    /*Broadcast Join :
        As customer Df is small we can broadcast it so that we can avoid shuffle stage
       if you don't specify the type of the join then default join is inner
    */

    //val broadcastDF = salesDf.join(broadcast(customerDf), Seq("customerId"))

    val broadcastCustDF = broadcast(customerDf)

    val broadcastDF = salesDf.join(broadcastCustDF, Seq("customerId"),"inner")
    broadcastDF.show()

    // cross join
    salesDf.crossJoin(customerDf).show()

    // SQL Queries
    salesDf.createOrReplaceTempView("Sales")
    customerDf.createOrReplaceTempView("Customer")

    println("Native SQL Syntax:1")

    val query = "SELECT s.*, c.name FROM Sales s, Customer c where s.customerId == c.customerId"

    spark.sql(query)//.show()

    println("Native SQL Syntax:2")

    spark.sql(
      "SELECT * FROM Sales s JOIN Customer c ON s.customerId == c.customerId")
    .show()

    val leftQuery = "SELECT * FROM Sales s left join Customer c on s.customerId == c.customerId"

    spark.sql(leftQuery)
      .show()

    Thread.sleep(500000)
  }
}
