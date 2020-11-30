package org.training.spark.xother

import org.apache.spark.{SparkConf, SparkContext}
import org.training.spark.batch.utils.Sales

object CsvInRDD {

  def main(args: Array[String]) {

    /*
      In spark 1.x we will follow below approach
     */
    val conf = new SparkConf().setMaster(args(0)).setAppName("csv in rdd")
    val sc  = new SparkContext(conf)
    val salesRDD  = sc.textFile(args(1))
    val headerLine = salesRDD.first()
    val otherLines = salesRDD.filter(row => row != headerLine)
    val sales = otherLines.map(_.split(",")).map(p=> Sales(p(0).trim.toInt,
      p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble))
    println(sales.map(value => value.customerId).collect().toList)
  }

}
