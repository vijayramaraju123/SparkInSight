package org.training

import java.sql.SQLException
import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.io.Source

trait DemandPlanningConf extends App {

  lazy val log =org.apache.log4j.LogManager.getLogger(getClass)


  var conn, memsqlConn: java.sql.Connection = null
  var sparkContext: org.apache.spark.SparkContext = null
  var sqlContext: org.apache.spark.sql.SQLContext = null
  var sparkSession: org.apache.spark.sql.SparkSession = null
  var rootLogger: org.apache.log4j.Logger = null
  var appProperties: Properties = null

  try {
    val conf:org.apache.spark.SparkConf= new SparkConf().setAppName(getClass.getName)
    sparkSession = SparkSession.builder.appName("spark session example").enableHiveSupport().config(conf).getOrCreate()
    sparkContext = sparkSession.sparkContext
    sparkContext.hadoopConfiguration.set("fs.alluxio.impl", "alluxio.hadoop.FileSystem")
    sqlContext = new SQLContext(sparkContext)
//    sqlContext.setConf("targetCluster/spark.cassandra.connection.host", Constants.CASSANDRA_TARGET)

 //   val connector = CassandraConnector(sparkContext.getConf)
    rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    try {
      val props = new Properties()
      props.setProperty("user","root")
      props.setProperty("password","cloudera")

      val jdbcDF1 = sparkSession.read.jdbc("jdbc:mysql://localhost:3306/ecommerce","kafka_sales",props)

      jdbcDF1//.show()} catch {
    } catch {
      case t: SQLException =>
        t.printStackTrace() // TODO: handle error
        log.error("Error Creating PostgresMemSQL Connection", t); // to capure the error message
    }

    appProperties = loadAppProperties();

  } catch {
    case a: Throwable =>
      println("Not a runtime exception"); log.error("", a);
  }
  def loadAppProperties(): Properties = {
    var properties: Properties = null
    val url = getClass.getResource("/DemandPlanning.properties")  /////// resources :load all sql informtion
    if (url != null) {
      val source = Source.fromURL(url)
      properties = new Properties()
      properties.load(source.bufferedReader())
      source.close()
    }
    properties
  }

object Appbuilder
  def apply(): DemandPlanningConf = new DemandPlanningConf {}

}
