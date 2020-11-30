package org.training.spark.xother.hiveintegration

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


object HivePartitionWrite extends App {

  val conf = new SparkConf().setMaster(args(0)).setAppName("Hive partition write")

  val sc = new SparkContext(conf)

  System.setProperty("javax.jdo.option.ConnectionURL",
    "jdbc:mysql://localhost/metastore?createDatabaseIfNotExist=true")
  System.setProperty("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver")
  System.setProperty("javax.jdo.option.ConnectionUserName", "hive")
  System.setProperty("javax.jdo.option.ConnectionPassword", "cloudera")
  System.setProperty("hive.metastore.warehouse.dir", "hdfs://quickstart.cloudera:8020/user/hive/warehouse")

  val hc = new HiveContext(sc)

  val loadOptions = Map("header" -> "true", "inferSchema" -> "true", "delimiter" -> ",")

  val salesDF = hc.read.format("csv").options(loadOptions).load(args(1))

  salesDF.printSchema()
  salesDF.show

  hc.sql("create table if not exists learning.itemid_partitions2(transactionId int, customerId int, amountPaid double) partitioned by(itemId int)")

  hc.setConf("hive.exec.dynamic.partition", "true")
  hc.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
  //salesDF.registerTempTable("temp")
  val salesDF1 = salesDF.withColumnRenamed("itemId", "itemid")
  salesDF1.printSchema()
  salesDF1.write.mode("append").partitionBy("itemid").saveAsTable("learning.itemid_partitions2")
  //salesDF.withColumn("itemid", col("itemId")).registerTempTable("temp")
  salesDF1.registerTempTable("temp")
  hc.sql("insert into learning.itemid_partitions partition(itemid) select transactionId, customerId, amountPaid, itemid from temp")
//  println(hc.table("learning.itemid_partitions").count())
//  hc.table("learning.itemid_partitions").show()
}
