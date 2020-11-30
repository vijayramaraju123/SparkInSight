package org.training.spark.xother.database

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}

object MySqlRead {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("spark_jdbc_read")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val mysqlOption = Map("url" -> "jdbc:mysql://localhost:3306/ecommerce","user"->"root", "password"->"cloudera")
    //val mysqlOption = Map("url" -> "jdbc:mysql://localhost:3306/ecommerce", "dbtable" -> "sales","user"->"hduser","password"->"training")

    val jdbcDF = sqlContext.read.format("jdbc").options(mysqlOption).option("dbtable" , "sales_date").load()
    //val jdbcDF = sqlContext.read.format("jdbc").options(mysqlOption).load()

    val prop = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","cloudera")

    val jdbcDF1 = sqlContext.read.jdbc("jdbc:mysql://localhost:3306/ecommerce","sales",prop)

    jdbcDF.printSchema()

    jdbcDF.show()
//    jdbcDF1.show()

    //Thread.sleep(100000)
  }
}
