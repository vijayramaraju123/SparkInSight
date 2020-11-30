package org.training.spark.xother.database

import org.apache.spark.{SparkConf, SparkContext}

object ParquetRead {

  def main (args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("parquet_read")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val salesDF = sqlContext.read.parquet(args(1))

    salesDF.show
    salesDF.registerTempTable("sales")
    sqlContext.sql("SELECT itemId from sales").show()
    //salesDF.write.save()
  }
}
