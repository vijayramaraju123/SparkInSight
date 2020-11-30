package org.training.spark.xother.database

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hduser on 29/7/16.
 */
object JDBCPredicatePush {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("spark_jdbc")
    val sc  = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val properties:Properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","cloudera")

    val amount = args(1)

    val query = s"(select customerId from sales where amountPaid > $amount) tmp"
    val jdbcDF = sqlContext.read.jdbc("jdbc:mysql://localhost:3306/ecommerce", query, properties)

    jdbcDF.printSchema()
    jdbcDF.show()
    //jdbcDF.write.mode("append").format("com.databricks.spark.csv").option("delimiter", ";").save("src/main/resources/csv_output")

    Thread.sleep(100000)
  }
}
