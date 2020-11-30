package org.training.spark.xother.database

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import java.io.{FileNotFoundException, IOException}

import scala.collection.immutable.Map
import scala.collection.JavaConverters._



object MySqlWrite {
  def main(args: Array[String]) {

    try {

      val conf = new SparkConf().
        setAppName("spark_jdbc_write").
        setMaster(args(0))
      val sc: SparkContext = new SparkContext(conf)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)

      val salesDf = sqlContext.read.
        format("csv").
        option("header", "true").
        option("inferSchema", "true").load(args(1))

      val option = Map("url" -> "jdbc:mysql://localhost:3306/ecommerce", "dbtable" -> "customer_total","user"->"root", "password"->"cloudera")

      val properties = new Properties()
      properties.setProperty("user", "root")
      properties.setProperty("password", "cloudera")

      salesDf.printSchema()
      salesDf.show()

      salesDf.withColumn("date", current_date()).write
        .mode("append")
        .jdbc("jdbc:mysql://localhost:3306/ecommerce", "sales_date", properties)

//      salesDf.where("amountPaid>500")//.groupBy("customerId").agg(sum("amountPaid").as("total"))
//        .write.jdbc("jdbc:mysql://localhost:3306/ecommerce", "sales_new", properties)

    }
    catch {
      case e : IOException => { e.printStackTrace(); e.getMessage }
      case e : FileNotFoundException => { e.printStackTrace(); e.getMessage }
    }
    finally {

    }

  }
}
