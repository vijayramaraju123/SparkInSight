package org.training.spark.xother.database

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}


object CassandraRead {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()

    val master =  args(0)
    sparkConf.setMaster(master)
    sparkConf.setAppName("Cassandra Read")

    sparkConf.set("spark.cassandra.connection.host", "localhost")
    //sparkConf.set("spark.cassandra.connection.keep_alive_ms", "40000")

    val sc: SparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val cassandraOptions = Map( "table" -> "sales", "keyspace" -> "demo")

    val cityStatsDF = sqlContext.read.format("org.apache.spark.sql.cassandra")
      .options(cassandraOptions)
      .load()

    cityStatsDF.show()

//    cityStatsDF.registerTempTable("sales")
//
//    val citydata = sqlContext.sql("Select * from sales")
//    citydata.show()
//
//    val cassandraOptions1 = Map( "table" -> "test", "keyspace" -> "ecommerce")
//    //citydata.write.format("org.apache.spark.sql.cassandra")
      //.options(cassandraOptions1).mode(SaveMode.Overwrite).save()
  }
}
