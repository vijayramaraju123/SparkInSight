package org.training

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.{Calendar, ConcurrentModificationException, TimeZone}

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.joda.time.{DateTime, Seconds}


/**
 * @author Pooja Nair
 *  Contains util method related to CloudSCM Application
 *
 */
object Utilities extends DemandPlanningConf {

  implicit class StringExtensions(val input: String) {
    def isNullOrEmpty: Boolean = input == null || input.trim.isEmpty || input.trim.equalsIgnoreCase("null")
  }

  def calculateRunTime(startDate: DateTime, endDate: DateTime): String = {
    val seconds = Seconds.secondsBetween(startDate, endDate).toString().substring(2)
    seconds
  }

  def getCurrentLocalDateTimeStamp(): String = {
    val currentDateTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
    currentDateTime
  }

  def getCurrent_IST_TimeStamp(): Timestamp = {
    val currentdate = Calendar.getInstance();
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    val obj = TimeZone.getTimeZone("IST");
    formatter.setTimeZone(obj);
    /*println("Local:: " + currentdate.getTime());
    println("IST:: " + formatter.format(currentdate.getTime()));*/
    val formattedTimestamp = formatter.format(currentdate.getTime())
    val currTime = java.sql.Timestamp.valueOf(formattedTimestamp);
    currTime
  }

  def getDateBeforeXDays(days: Integer): String = {
    var today = Calendar.getInstance();
    today.add(Calendar.DATE, -days);
    val date = new java.sql.Date(today.getTimeInMillis());
    var formatter: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd");
    var dateret = formatter.format(date)
    dateret
  }

  def getDateBeforeXMonths(months: Integer): String = {
    var today = Calendar.getInstance();
    today.add(Calendar.MONTH, -months);
    val date = new java.sql.Date(today.getTimeInMillis());
    var formatter: SimpleDateFormat = new SimpleDateFormat("yyyyMM");
    var dateret = formatter.format(date)
    dateret + "01"
  }

  /*def fileNameToTableName(fileName: String): String = {
    if (fileName.contains(Constants.CONFIG_HEADER)) {
      return Constants.TEMP_CONFIG_HEADER
    } else if (fileName.contains(Constants.CONFIG_DETAIL)) {
      return Constants.TEMP_CONFIG_DETAIL
    } else if (fileName.contains(Constants.SI_MAINTENANCE)) {
      return Constants.TEMP_SLI_MASTER
    } else if (fileName.contains(Constants.HIERARCHY_MAINTENANCE)) {
      return Constants.TEMP_PPH
    } else if (fileName.contains(Constants.ITEM_ALIAS)) {
      return Constants.TEMP_ITEMALIAS
    } else if (fileName.contains(Constants.PLI_LINKING_RULES)) {
      return Constants.TEMP_PLILINKINGRULES
    } else {
      return ""
    }
  }*/

  def loadResourceToString(requestedQuery: String): String = {
    val queryString = appProperties.getProperty(requestedQuery)
    queryString
  }

  def isNullOrEmpty[T](s: Seq[T]) = s match {
    case null  => true
    case Seq() => true
    case _     => false
  }
  
  def deleteDataFrame(df: DataFrame, sc: SparkContext, tableName: String, pkarr: Array[String]) = {

  /*  val connector = CassandraConnector(sc.getConf)
    df.rdd.foreach({
      x =>
        var pkRowArr = x.toSeq.toArray

        var delQuery = "delete from " + Constants.CASSANDRA_KEYSPACE + "." + tableName + " where "
        var where = ""
        var len = pkRowArr.length - 1
        for (i <- 0 to len) {
          where = where + pkarr(i) + " = '" + pkRowArr(i) + "' and "
          println("the where condition is " + where)
        }
        delQuery = delQuery + where + "and"
        delQuery = delQuery.replace("and and", "")
        println("delQuery -->" + delQuery)
        try {
          connector.withSessionDo { session => session.execute(delQuery) }
        } catch {
          case t: Throwable =>
            t.printStackTrace() // TODO: handle error
          // log.error(t.getMessage)
        }

    })*/
  }
  
  
  def is_active(sqlContext: SQLContext){
   
     println("reading from config_header table")
    sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("cluster" -> "targetCluster", "table" -> "master_configuration_header", "keyspace" -> Constants.CASSANDRA_KEYSPACE)).load().createOrReplaceTempView("master_configuration_header")
    var finaldf = sqlContext.sql("select f.config_id, f.config_id_prefix ,CASE WHEN isNullOrEmpty(f.is_active) THEN 'ACTIVE' ELSE f.is_active END as is_active  from master_configuration_header f ")
      finaldf.show()
    
      finaldf.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("table" -> "master_configuration_header", "keyspace" -> Constants.CASSANDRA_KEYSPACE)).save()
  //    writeToMemsqlWithOutSession(sqlContext, "master_configuration_header", finaldf, NonCSPconfigPKKeys.MASTER_HEADER_PK.toString())
      
 }

  def userBussinessValidation(BusinessId: String, fileName: String, dataframe: DataFrame, currentUser: String, tableId: String, redisFlag: Boolean): Array[DataFrame] = {
    var df = dataframe
    var validdf: DataFrame = null
    var invaliddf: DataFrame = null
    var array = Array(validdf, invaliddf)
    try {

      if (fileName.contains(fileName)) {

        if (df.count > 0) {

          df.createOrReplaceTempView("test")

          validdf = sqlContext.sql("select * from test  where business_unit_id='" + BusinessId + "'")
          invaliddf = df.except(validdf)
          array = Array(validdf, invaliddf)
          println("user validation completed")
        }
      }
    } catch {
      case e @ (_: NullPointerException | _: IllegalArgumentException | _: ConcurrentModificationException) =>
        log.error("--Basic Exception in Loading CSV : ", e);
        println("--Basic Exception in Loading CSV : " + e)
      case e @ (_: NoSuchElementException | _: UnsupportedOperationException | _: IndexOutOfBoundsException) =>
        log.error("--Exception in Loading CSV Arrray access : ", e);
        println("--Exception in Loading CSV Arrray access : " + e)
      case t: Throwable =>
        println("Not a runtime exception" + t.printStackTrace())
/*        if (redisFlag) {
          userFilesUpdateToRedis(tableId.toInt, currentUser, 4, Constants.ERROR_EXISTS, true, Constants.SYSTEM_ERR)
          userFilesUpdateToRedis(tableId.toInt, currentUser, 6, Constants.COMPLETE, true, "")
        }*/

    }
    array
  }
}
