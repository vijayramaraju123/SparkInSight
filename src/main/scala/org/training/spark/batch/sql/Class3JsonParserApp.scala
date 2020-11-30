package org.training.spark.batch.sql

import org.apache.spark.sql.SparkSession

object Class3JsonParserApp {

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName(getClass.getName)
      .master("local")
      .getOrCreate()

    val sqlContext = spark.sqlContext

    println(getClass.getName)

    /*
      Reading Json files
     */

    //val sales = sqlContext.read.format("json").load(args(1))
    //val sales = spark.read.option("multiline","true").json(args(0))
    val sales = spark.read.json(args(0))

    sales.printSchema()
    sales.show()

    // Writing JSON files
    val outputDf = sales
      .selectExpr("transactionId orderId","customerId","itemId","amountPaid","amountPaid*0.2 discount")

    outputDf.coalesce(1).write.format("json").save("src/main/resources/output/json/json1")
    outputDf.write.json("src/main/resources/output/json/json2")

    //Thread.sleep(1000000)
  }

}

