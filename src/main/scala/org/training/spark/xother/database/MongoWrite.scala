package org.training.spark.xother.database

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._


object MongoWrite {
   def main (args: Array[String]) {

     val conf = new SparkConf().setMaster(args(0)).setAppName("spark_mongo_write")
     val sc: SparkContext = new SparkContext(conf)

     val sqlContext = new org.apache.spark.sql.SQLContext(sc)

     val salesDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(args(1))

     val options = Map("host" -> "localhost:27017", "database" -> "ecommerce", "collection" -> "sales")

     salesDf.write.format("com.stratio.provider.mongodb").mode(SaveMode.Append).options(options).save()

  }
}
