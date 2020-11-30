package org.training

/*
 * @author Laila
 */
import java.util.concurrent.{Callable, ExecutorService, Executors, FutureTask}

import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

class GenerateOrdersAndLink(sparkSession: SparkSession, sparkContext: SparkContext, sqlContext: SQLContext) {
@transient lazy val log = org.apache.log4j.LogManager.getLogger(getClass)
 val auditRow = new ArrayBuffer[Row]
  def callOrders() {

    cacheAndLoadOrderTables();

    val generateSliOrders = new GenerateSliOrders(sqlContext)
    val gS= generateSliOrders.execute()
  //  println("TotalOrders Return---->" +gS)

    val Itemalias_Final = new ItemAliasFinal(sqlContext)
    val iAF = Itemalias_Final.execute()

   val linkingStageService = new GeneratePLILinkingStageData(sqlContext, sparkContext)
    val lSS=linkingStageService.execute()
    //println("GeneratePLILinkingStageData Return---->" +lSS)

    val ordersPliService = new GeneratePLIOrdersData(sqlContext, sparkContext)
    val oPS = ordersPliService.execute()
    println(oPS)

//    val generateOrders = new TotalOrders(sparkSession, sqlContext, sparkContext)
 //   val gAO = generateOrders.execute()
     //println("TotalOrders Return---->" +gAO)

 //   val generateDeliveries = new DailyDeliveries(sparkSession, sqlContext, sparkContext)*/
    
    //println("TotalOrders Return---->" +gD)

    var actionPool: ExecutorService = Executors.newFixedThreadPool(5)

     println("Starting First Actionpool---")
//      val action1 = new FutureTask[Unit](new Callable[Unit]() { def call(): Unit = { val gAO = generateOrders.execute();  println("TotalOrders Return---->" +gAO); auditRow += gAO; println("Method1--->")} })
  //    val action2 = new FutureTask[Unit](new Callable[Unit]() { def call(): Unit = { val gD = generateDeliveries.execute(); println("TotalOrders Return---->" +gD); println("Method2--->") } })

    val action1 = new FutureTask[Unit](new Callable[Unit]() { def call(): Unit = { val gAO = Itemalias_Final.execute();  println("TotalOrders Return---->" +gAO); auditRow += gAO; println("Method1--->")} })
    val action2 = new FutureTask[Unit](new Callable[Unit]() { def call(): Unit = { val gD = Itemalias_Final.execute(); println("TotalOrders Return---->" +gD); println("Method2--->") } })

    actionPool.execute(action1)
      actionPool.execute(action2)
      action1.get()
      action2.get()
      actionPool.shutdown()
     
      /*dropTempView(sqlContext, sparkSession, ArrayBuffer("alcr_master", "part", "independentdemand", "referencepart",
      "u_schedulelinecategory", "demandorder", "historicaldemandactual", "historicaldemandheader",
      "historicaldemandcategory", "site", "calendardate", "master_configuration_detail", "master_salesitem_maintenance",
      "master_itemalias", "master_dummyksi",
      "sliorders", "master_plilinkingrules", "itemalias_final",
      "plilinking_stage", "orderspli", "orderspli_allocation"))*/

    /*var tableArr = ArrayBuffer("sliorders", "orderspli", "itemalias_final", "plilinking_stage", "orders_pli_allocation")

    writeTablesToCassandra(tableArr)*/
   auditRow += lSS;
 //   auditRow += gS;
    auditRow += iAF;
    for( a <- oPS ){
         println( "Value of a: " + a );
         auditRow += a;
      }
      for( a <- gS ){
         println( "Value of a: " + a );
         auditRow += a;
      }
    println(auditRow)
    writeReport();
  }
  def cacheAndLoadOrderTables() = {
    println("..Caching required tables..")
    var tablearr = ArrayBuffer("part", "independentdemand", "referencepart", "u_schedulelinecategory", "demandorder", "historicaldemandactual", "historicaldemandheader", "historicaldemandcategory", "site", "calendardate")
 //   cacheTablesInSparkMemory(sparkContext, sqlContext, tablearr, Constants.CLOUDEDH_KEYSPACE)
    tablearr = ArrayBuffer("alcr_master", "master_planning_product_hierarchy", "master_configuration_detail", "master_salesitem_maintenance", "master_itemalias", "master_plilinkingrules", "master_dummyksi")
 //   cacheTablesInSparkMemory(sparkContext, sqlContext, tablearr, Constants.CASSANDRA_KEYSPACE)
    println("..Completed Caching required tables..")

  }

  def writeTablesToCassandra(tableArr: ArrayBuffer[String]) = {
    println("-- Starting writing tables..")
    for (tableName <- tableArr) {
      val df = sqlContext.table(tableName)
      df.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Overwrite).options(Map("cluster" -> "targetCluster", "table" -> tableName, "keyspace" -> Constants.CASSANDRA_KEYSPACE)).save()
      println("-- completed writing table.." + tableName)
    }
    println("-- completed writing tables..")
  }
  def writeReport() = {
    try {
      //sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("cluster" -> "targetCluster", "table" -> Constants.ORDER_STATISTICS_TBL, "keyspace" -> Constants.CASSANDRA_KEYSPACE)).load().createOrReplaceTempView(Constants.ORDER_STATISTICS_TBL)
      val rdd = sparkContext.parallelize(auditRow.toSeq)
      val aStruct = StructType(Array(StructField("tablename", StringType), StructField("timetaken", StringType),
        StructField("count", LongType), StructField("status", BooleanType), StructField("message", StringType), StructField("generatedate", TimestampType)))
      val aNamedDF = sqlContext.applySchema(rdd, aStruct)
      aNamedDF.show
      //aNamedDF.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("table" -> Constants.ORDER_STATISTICS_TBL, "keyspace" -> Constants.CASSANDRA_KEYSPACE)).save()
      println("Completed Order Report Statistics")
    } catch {
      case t: Throwable =>
        t.printStackTrace() // TODO: handle error
        log.error("Exception in Order Report Statistics", t);
    }
  }
}
