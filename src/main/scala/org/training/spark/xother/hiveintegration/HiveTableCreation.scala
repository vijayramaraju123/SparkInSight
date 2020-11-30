package org.training.spark.xother.hiveintegration

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext


object HiveTableCreation {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster(args(0)).setAppName("hive metastore")
    val sc : SparkContext = new SparkContext(conf)
    //System.setProperty("hive.metastore.uris", "thrift://localhost:9083");

    System.setProperty("javax.jdo.option.ConnectionURL",
    "jdbc:mysql://localhost/metastore?createDatabaseIfNotExist=true")
    System.setProperty("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver")
    System.setProperty("javax.jdo.option.ConnectionUserName", "hive")
    System.setProperty("javax.jdo.option.ConnectionPassword", "cloudera")
    System.setProperty("hive.metastore.warehouse.dir", "hdfs://quickstart.cloudera:8020/user/hive/warehouse/")

    val hiveContext = new HiveContext(sc)

    //hiveContext.setConf("hive.metastore.uris", "thrift://localhost:9083");
    //hiveContext.sql("show tables").show()

    val sales = hiveContext.read.format("com.databricks.spark.csv")
      .option("header", "true").load(args(1))

//    sales.write.mode("append").saveAsTable("learning.sales_app")

    //sales.write.insertInto("learning.sales_app")

    sales.registerTempTable("test")

    hiveContext.sql("use learning")

    hiveContext.sql("INSERT INTO sales_app select * from test")
    //hiveContext.sql("SET hive.metastore.warehouse.dir=hdfs://localhost:54310/user/hive/warehouse");

    //hiveContext.sql("show tables").show()
    //hiveContext.sql("select count(*) from learning.sales1").show
    sales.write.mode(SaveMode.Overwrite).saveAsTable("sales1")
    //hiveContext.sql("create table sales2 as select * from test")
//    hiveContext.sql("show tables").show()
//    hiveContext.table("sales1").show()
    //println(hiveContext.table("learning.sales_app").count)

  }

}
