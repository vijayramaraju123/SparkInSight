package org.training

import java.util.concurrent.{Callable, ExecutorService, Executors, FutureTask}

import scala.collection.mutable.ArrayBuffer
// import com.overwrite.Overwrite
// source : from where we get the data :  cassandra
// buisness logic : related to supply management things, sales and order report.
// order followup resoprt
// cassandra saving data for reporting
//

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.joda.time.DateTime

import org.training.Utilities.calculateRunTime


class Processor(userInput: Array[String], sparkContext: SparkContext, sqlContext: SQLContext, sparkSession: SparkSession) extends Serializable {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger(getClass)
  val rootLogger = Logger.getRootLogger()
  val startDate: DateTime = DateTime.now()
  var comflag = false

  /**
   * method to redirect to DP process
   */

  def process(): String = {

    try {

      println("Hello")
      var fileName, migration_flag, currentUser = ""

      val inputType = userInput(0).toString
      
      if (userInput.length > 1) {
        fileName = userInput(1);
      }
      if (userInput.length > 2) {

        currentUser = userInput(2)
      }
      if (userInput.length > 3) {
        migration_flag = userInput(3)
      }
      var redisFlag = false

      println("starting DMTProcessor")
      val startDate: DateTime = DateTime.now()

      val generateOrdersAndLink = new GenerateOrdersAndLink(sparkSession, sparkContext, sqlContext)

      inputType match {

        /*DMT EXPORTS*/
   /*     case Constants.PRODUCT_MASTER_DATA_DMT             => product_master_dmt.execute()
        case Constants.CUSTOMER_MASTER_DATA_DMT            => customer_master_dmt.execute();
        case Constants.BULK_RELATION                       => dmt_bulk_relationship.execute()
        case Constants.DMT_CONFIG                          => dmt_configurations.execute()
        case Constants.PRICE_SI                            => Price_SI.execute()
        case Constants.OPPORTUNITY_DELTA                   => opportunityDelta.execute()
        case Constants.INACTIVE_CONFIGS                    => inactiveConfigurations.execute()

        /*BW EXPORTS*/

        case Constants.fALUOrders                          => fALUOrders.loadCtrrData(fileName)
        case Constants.fALUDeliveries                      => fALUDeliveries.loadCtrrData(fileName)
        case Constants.file_type1                          => fALUOrders.processInputFiles(inputType, fileName) // inputs => file1 and InputFileName
        case Constants.file_type2                          => fALUOrders.processInputFiles(inputType, fileName) // inputs => file2 and InputFileName
        case Constants.PRODUCT_MASTER_BW                   => product_master_bw.execute()
        case Constants.HISTORICAL_DMT                      => historicalDmt.execute(fileName)
        case Constants.CUSTOMER_MASTER_DATA_BW             => customer_master_bw.execute();
        case Constants.GKMYC                               => gkmyc.execute(fileName)

        /*CTRR Exports*/

        case Constants.TIMESERIES                          => timeseries.execute(fileName)

        /*E2PR Exports*/

        case Constants.ITEM_ADDT                           => item_addt_e2pr.execute()
        case Constants.CUSTOMER_E2PR                       => customer_e2pr.execute()

        /*IMPORTS*/
        case Constants.CUSTOMER_MASTER_DATA_P20            => cmd.load_p20_customer_hierarchy(fileName)
        case Constants.CUSTOMER_MASTER_DATA_ALU            => cmd.load_alu_customer_mapping(fileName)
        case Constants.CUSTOMER_MASTER_DATA_DMY            => cmd.load_dmy_customer(fileName)
        case Constants.TBL_ALCR_MAINTENANCE                => cmd.load_x_marking_alcr(fileName)
        case Constants.sCRM                                => opportunity.scrmOppurtunity(fileName)
        case Constants.CSP_CONFIG                          => csp_config.loadConfigHeader(sqlContext)*/
        case Constants.ORDERS                              => generateOrdersAndLink.callOrders()
      /*  case Constants.SLI_ATTRIBUTES                      => sli_attribute.execute(fileName)
        case Constants.DELETE_RELATIONS                    => deleteRelations.execute(fileName)

        /*Auto Creates*/
        case Constants.PROD_MASTER_CTRR                    => ProductMasterDataCTRR.process(fileName, sqlContext, migration_flag)
        case Constants.PS_AUTO                             => PricingSheets.process(fileName, sqlContext, migration_flag)
        case Constants.ALPIM_AUTOCREATE                    => alpimAutoCreate.execute()

        /*CSP and NONCSP INCOMING */

        case Constants.CONFIG_CSP_HEADER                   => NonCSPandCSPConfig.edh_conf_detail_process(sqlContext, fileName, currentUser)
        case Constants.CONFIG_CSP_DETAIL                   => NonCSPandCSPConfig.edh_conf_master_process(sqlContext, fileName, currentUser)

        /*Automation*/
        case Constants.DMT_EXPORTS_SLOT_1                  => executeParallel(Array(product_master_dmt, customer_master_dmt, dmt_configurations, dmt_bulk_relationship), comflag)
        case Constants.CTRR_EXPORTS | Constants.TIMESERIES => timeseries.execute(fileName)
        case Constants.E2PR_EXPORTS                        => executeParallel(Array(item_addt_e2pr, customer_e2pr), comflag)
        // case Constants.BW_EXPORTS_SLOT_1                   => executeParallel(Array(fALUOrders, fALUDeliveries), comflag)
        case Constants.BW_EXPORTS_SLOT_2                   => executeParallel(Array(product_master_bw, customer_master_bw), comflag)

        /*Migration*/

        case "AA_MIGRATION"                                => dataMigration.loadAAData()
        case "OPP_MIGRATION"                               => dataMigration.loadOpportunity(fileName)
        case "ACT_MIGRATION"                               => dataMigration.loadBulkRelations(fileName)
        case "ALCR_MIGRATION"                              => dataMigration.loadAlcrFile(fileName)
        case "default_pph"                                 => dataMigration.loadDefaultPph()
        case "CUST_EXCLUSION_MIGRATION"                    => dataMigration.loadCustomerExclusionFile(fileName)
        case "GK_MASTER"                                   => dataMigration.loadGkMasterFile(fileName)
        case "PMD"                                         => productmasterMigration.execute(fileName)
        case "CLEANUP_SLI"                                 => dataMigration.cleanupSalesItem()
        case Constants.ALL_REPORTS_GENERATOR               => reports.execute()
        case "DMYKSI"                                      => dataMigration.dummyksiUpdate()
        case "OVERRIDE"                                    => Overwrite.main(fileName, sqlContext)
*/
        case _ =>
          println("Invalid Entry")
      }

      println("end of " + inputType + "  ---> " + calculateRunTime(startDate, DateTime.now()))

      ""
    } catch {
      case t: Throwable => {
        t.printStackTrace()
       //   val email = new Email()
         //  email.MailPart(" Exception in :" + this.getClass.getSimpleName, t.getStackTraceString)
     " "
      }
    }
  }

  def executeParallel(genericObjs: Array[GenericDMTService], comflag: Boolean): Unit = {
    try {
      val actionPool1: ExecutorService = Executors.newFixedThreadPool(10)
      var futureArr = ArrayBuffer[FutureTask[String]]()

      genericObjs.foreach { y =>
        val className = y.getClass.getSimpleName.toString()
        val action1 = new FutureTask[String](new Callable[String]() {
          def call(): String = {
            execute(y, comflag)
            className
          }
        })
        actionPool1.execute(action1)
        futureArr += action1

      }

      futureArr.foreach { y =>
        println(s"Completed process for " + y.get + "   total time taken  " + calculateRunTime(startDate, DateTime.now()))

      }
      actionPool1.shutdown()
    } catch {
      case t: Throwable => {
        t.printStackTrace()
  //      val email = new Email()
   //     email.MailPart(" Exception in :" + this.getClass.getSimpleName, t.getStackTraceString)
      }
    }

  }
  def execute(genericObj: GenericDMTService, comflag: Boolean): Unit = {
    var flag = comflag
    val className = genericObj.getClass.getSimpleName.toString()
    flag = true
    println("starting " + className + "  ---> " + calculateRunTime(startDate, DateTime.now()))
    try {
      genericObj.execute()
      flag = true
    } catch {
      case t: Throwable =>
        t.printStackTrace() // TODO: handle error
        log.error(t.getMessage)
        flag = false
        println("ending " + className + "  ---> " + calculateRunTime(startDate, DateTime.now()))
    }
  }
}
