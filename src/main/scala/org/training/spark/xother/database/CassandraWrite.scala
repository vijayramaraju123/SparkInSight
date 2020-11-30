package org.training.spark.xother.database

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._


object CassandraWrite {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()

    val master =  args(0)
    sparkConf.setMaster(master)
    sparkConf.setAppName("CassandraWrite")

    sparkConf.set("spark.cassandra.connection.host", "localhost")
    sparkConf.set("spark.cassandra.connection.keep_alive_ms", "40000")

    val sc: SparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val cassandraOptions = Map( "table" -> "sales", "keyspace" -> "demo")
    /*val usersdf = sqlContext.read
                         .format("org.apache.spark.sql.cassandra")
                         .options(cassandraOptions)
                         .load()*/

    val salesDf = sqlContext.read.format("csv")
      .option("header", "true").
      option("inferSchema", "true")
      .load(args(1))
   //usersdf.registerTempTable("users")
    //val adults = sqlContext.sql("Select * from users where age > 21").show()
    //usersdf.select("age", "user_id", "name").show()

//    salesDf.select(salesDf("transactionId").alias("tansaction_id"), salesDf("itemId").alias("itemid"))
//      .withColumn("date",current_timestamp())
//      .write.format("org.apache.spark.sql.cassandra")
//      .options(cassandraOptions)
//      .option("confirm.truncate","true")
//      .mode(SaveMode.Overwrite)
//      .save()

    /* Cassandra commands
    create keyspace ecommerce with replication = { 'class':'SimpleStrategy', 'replication_factor':1} ;
    create table sales1(tansaction_id int, itemid TEXT, date timestamp, primary key(tansaction_id));
     */

    salesDf.toDF("tid", "custid", "itemid", "price")
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(cassandraOptions)
      .mode(SaveMode.Append)
      .save()

  }
}
