package org.training.spark.xother

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{lit, udf}
import org.apache.spark.{SparkConf, SparkContext}

object UDFExamples {

  //def myToInt (input: Double) = input.toInt

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("UDFExample")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val salesDf = sqlContext.read.format("org.apache.spark.sql.json")
      .load(args(1))
                  .withColumn("tax",lit(200.toDouble))

    salesDf.show()

    def sum(x:Double,y:Double) = x+y

    val sumColsUdf = udf(sum _)

    val sumUdf = udf((a:Double,b:Double) => a+b)

    salesDf.select($"transactionId",$"amountPaid",$"tax",sumColsUdf($"amountPaid",$"tax"))
      //.show

     salesDf.registerTempTable("sales")

    sqlContext.udf.register("sumUdf1",sum _)

    sqlContext.sql("select sumUdf1(amountPaid,tax) total from sales").show()


//    def toInt(input:Double) = input.toInt
//
//    toInt(10.0)

//    val myUDF= udf(toInt _)
//
//    def concat(x:Any,y:Any) = s"$x $y"
//
//    val concatUDF = udf(concat _)
//
//    salesDf.select($"customerId", myUDF($"amountPaid"),
//      concatUDF($"itemId",$"amountPaid"),
//      concat_ws("-",$"itemId",$"amountPaid"))
//      //.show()

    //val myToInt = udf()

//    sqlContext.udf.register("ConvertToInt", toInt _)
//
//    val results = sqlContext.sql("select customerId,ConvertToInt(amountPaid) from sales")


    //salesDf.select($"customerId", myToInt($"amountPaid")).show()
//    results.show()

  }

}
