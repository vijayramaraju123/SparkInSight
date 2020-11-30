package org.training.spark.xother

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object UdfFunction {

  def priceIncreaseDef (price: Double) = {
    price + 50.0
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("dataframe_udf")
    val sc : SparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val salesDf = sqlContext.read.format("org.apache.spark.sql.json").load(args(1))
    salesDf.registerTempTable("sales")

    val resultBefore = salesDf.select("customerId", "amountPaid").show()


   /*val priceIncreaseUdf = (price:Double) => {
     price + 50.0
   }*/


    //val priceIncrease = udf((price:Double) => price + 50.0)
    //val priceIncrease = udf(priceIncreaseUdf)
    // use existing definition is available in some other package as udf
    //val priceIncrease = udf(priceIncreaseDef _)

    sqlContext.udf.register("priceincrease", priceIncreaseDef _ )
    //val results = salesDf.select($"customerId", priceIncrease(col("amountPaid")) as "IncreasedValue")
    val results1 = sqlContext.sql("select customerId,priceincrease(amountPaid) from sales")
    //println("dataframe DSL api usage")
    //results.show()
    //println("sql query usage")
    results1.show()

  }
}
