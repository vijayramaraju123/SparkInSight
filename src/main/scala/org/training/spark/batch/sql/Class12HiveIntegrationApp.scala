package org.training.spark.batch.sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/*Make sure below services are running before running this program
  1) Start HDFS using below command
  cd /home/cloudera
    ./start-hdfs.sh
  2) Start Hive using below command
    ./start-hive.sh
 ---------------------------------------
  Make sure to add the below property
  System.setProperty("hive.metastore.uris", "thrift://localhost:9083")
  ------------------------------------------
  The Hive Metastore, also referred to as HCatalog is a relational database repository containing metadata about objects you create in Hive.
  When you create a Hive table, the table definition (column names, data types, comments, etc.) are stored in the Hive Metastore.
  This is automatic and simply part of the Hive architecture.

  The reason why the Hive Metastore is critical is because it acts as a central schema repository which can be used by other access tools
  like Spark. We need the hive metastore to run queries with spark because spark will use that metastore to execute queries

  Additionally, through Hiveserver2 you can access the Hive Metastore using ODBC and JDBC connections.
  This opens the schema to visualization tools like PowerBi or Tableau.

  There are three modes of configuring a metastore:
    1)Embedded
    2)Local
    3)Remote
 */

object Class12HiveIntegrationApp extends App {

  System.setProperty("hive.metastore.uris", "thrift://localhost:9083")
  //System.setProperty("hive.metastore.uris", "thrift://192.168.79.174:9083")

  val ss = SparkSession
    .builder()
    .enableHiveSupport() //This look's at hive-site.xml to connect to Hive
    .appName("Class12HiveIntegrationApp")
    .master("local[4]")
    .getOrCreate()

  val csvDF = ss.read
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("src/main/resources/datasets/sales.csv")


  ss.sql("SHOW DATABASES")//.show()
  ss.sql("USE LEARNING")
  ss.sql("SHOW TABLES")//.show()

  /*
    Create Hive Table from DataFrame
  */

  val salesDF = csvDF.withColumn("createDate",current_date())

  //salesDF.write.format("orc").saveAsTable("learning.sales_july")

  /*Override will truncate and load data
  Append will add data to existing table
  */

  //salesDF.write.mode(SaveMode.Append).saveAsTable("learning.sales")

//  ss.table("learning.sales").show()
//  println("sales table row count : " + ss.table("learning.sales").count())

  //Specifying mode and format
  //salesDF.write.format("orc").mode(SaveMode.Overwrite).saveAsTable("learning.sales_orc")

//  ss.table("learning.sales_orc").show()
//  println("sales table row count : " + ss.table("learning.sales_orc").count())

  /*Loading data from hive table*/

  val hiveDF = ss.table("learning.sales")
    .selectExpr("transactionId", "amountPaid")

  //hiveDF.show()

  /*Loading required columns data from hive table*/

  val hiveSelectDF = ss.sql("select transactionId, amountPaid from learning.sales")

  //hiveSelectDF.show()

  /*
    Writing partitioned table on HIVE
    we need to enable below hive properties
   */
  ss.sqlContext.setConf("hive.exec.dynamic.partition", "true")
  ss.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

  /* Approach 1 : DSL save As partition Table :
  creates the table structure and stores the first version of the data. However,
  the overwrite save mode works over all the partitions even when dynamic is configured.
 */
    //single partition column
  salesDF.write
    .mode("overwrite")
    .format("orc")
    .partitionBy("itemId")
    .option("compression","none")
    //.saveAsTable("learning.sales_item_partition")

  //multiple partition columns
  salesDF.write
    .mode("overwrite")
    .format("orc")
    .partitionBy("createDate","itemId")
    .option("compression","none")
    //.saveAsTable("learning.sales_date_item_partition")

  /*Approach 2 : using HQL queries
    Validate whether tables are created or not by executing below commands
    hadoop dfs -ls /user/hive/warehouse/learning.db/item_partition
   */

  salesDF.createOrReplaceTempView("sales")

//  ss.sql("drop table if exists learning.sales_partition_hql")
//
//  ss.sql("""create table learning.sales_partition_hql (transactionid int, customerid int, amountpaid double, createdate date)
//      |partitioned by (itemid int)
//      |stored as orc
//      |""".stripMargin)
//
//  ss.sql("insert overwrite table learning.sales_partition_hql partition(itemid) select transactionid,customerid,amountpaid,createdate,itemid from sales")

  /*insertInto: does not create the table structure, however, the overwrite save mode works only the needed partitions when dynamic is configured.*/

  salesDF.select("transactionid","customerid","amountpaid","createdate","itemid")
    .write
    .mode("append")
    //.insertInto("learning.sales_partition_hql")


  //Creating bucketed tables from Spark

  salesDF.write
    .mode(SaveMode.Overwrite)
    .bucketBy(2,"customerId")
    .format("orc")
    //.saveAsTable("learning.sales_bucket")

  salesDF.write
    .mode(SaveMode.Overwrite)
    .bucketBy(2,"customerId","itemId")
    .format("orc")
    //.saveAsTable("learning.sales_bucket")

  //Creating partition and bucketed tables from Spark

  salesDF.write
    .mode(SaveMode.Overwrite)
    .partitionBy("itemId")
    .bucketBy(2,"customerId")
    .format("orc")
    //.saveAsTable("learning.sales_part_bucket")

  ss.table("learning.sales_part_bucket").show()

  //Thread.sleep(50000)
}
