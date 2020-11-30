package org.training.spark.xother.database

import org.apache.spark.{SparkConf, SparkContext}


object MongoRead {

  def main (args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("spark_mongo_write")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val options = Map("host" -> "localhost:27017", "database" -> "ecommerce", "collection" -> "sales")

    val salesDF = sqlContext.read.format("com.stratio.provider.mongodb").options(options).load

    salesDF.show

  }
}
