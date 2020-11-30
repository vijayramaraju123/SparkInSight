package org.training

/*
 * @author Laila
 */
import java.util.concurrent.{Callable, ExecutorService, Executors, FutureTask}

import org.apache.spark.sql.{Row, SQLContext}
import org.joda.time.DateTime

import org.training.Utilities.{ calculateRunTime , getCurrent_IST_TimeStamp}


import scala.collection.mutable.ArrayBuffer

class GenerateSliOrders(sqlContext: SQLContext) {
 @transient lazy val log = org.apache.log4j.LogManager.getLogger(getClass)
 val audit = new ArrayBuffer[Row]
	def generateSLIOrdersFromInd() : ((Long, Boolean, String)) =  {
		println("--Generating SLI Orders IND")
		var dfCount: Long = 0L; var status: Boolean = false; var exceptionMsg: String = "";

		try {
			val df = sqlContext.sql(s"SELECT ind.line AS line,ind.order_id AS order_id,ind.part_name AS part_name,ind.part_site AS part_site,ind.u_actual_ship_date AS u_actual_ship_date,ind.u_crd_ship AS u_crd_ship,ind.quantity AS quantity,(CASE WHEN LOWER(ind.STATUS) IN ('historical','shipment')THEN ind.u_orig_quantity ELSE 0 END) AS shippedqty,do.customer_id AS customer_id,do.customer_site As customer_site,ind.STATUS AS status,(CASE WHEN LOWER(ind.STATUS) IN ('historical','shipment')THEN ind.quantity ELSE 0 END) AS closed_quantity,ind.confirmbydate,ind.promiseddate,ind.requestdate,ind.u_acd_mad,ind.u_crd_mad,ind.u_booked_date as u_createdate,ind.u_custorderid,ind.u_matlgr4,ind.u_po_line,ind.u_ponumber,ind.u_profit_center,ind.u_warehouse_source,ind.u_business_unit as u_business_unit,extractString(u_workorder,'_') AS what, ind.u_hl_item,ind.u_userfield_04 as vendorcode,do.u_salesorderuserstatus FROM master_salesitem_maintenance sli JOIN master_planning_product_hierarchy pph on sli.net_element_version_id = pph.net_element_version_id JOIN independentdemand ind ON sli.sales_item_id = ind.part_name JOIN part p ON p.NAME = ind.part_name AND ind.part_site = p.site JOIN referencepart rp ON p.referencepart = rp.value LEFT JOIN u_schedulelinecategory uschline ON ind.u_schedulelinecategory = uschline.id JOIN demandorder do ON ind.order_id = do.id AND ind.order_type = do.type AND ind.order_type_controlset = do.type_controlset WHERE pph.business_unit_id in ('APPLICATION') AND NOT (LOWER(p.type) LIKE 'what%' OR LOWER(p.type) LIKE 'bill%') AND (CASE WHEN ind.u_userfield_05 IS NULL THEN TRUE ELSE (lower(ind.u_userfield_05) NOT LIKE 'iso_exclude%')END) AND (LOWER(ind.order_type) LIKE 'cso%' OR LOWER(ind.order_type) LIKE 'rli' OR LOWER(ind.order_type) LIKE 'shipment%' OR LOWER(ind.order_type) LIKE 'lent%' OR LOWER(ind.order_type) LIKE 'iso%' OR ind.STATUS IN ('historical%','shipment%','shipment%'))AND (LOWER(ind.STATUS) LIKE 'what_comp%' OR NOT(LOWER(ind.STATUS) LIKE 'what%' OR LOWER(ind.STATUS) LIKE 'bill%'))AND (LOWER(uschline.u_schedulelinecategorytype) NOT LIKE 'billable%' OR rp.u_isrlii = TRUE) AND LOWER(ind.STATUS) NOT LIKE 'not_approved' AND LOWER(ind.order_type) NOT LIKE 'sto'").repartition(Constants.NO_OF_PARTITIONS)
	//		val df = sqlContext.sql(s"SELECT ind.line AS line,ind.order_id AS order_id,ind.part_name AS part_name,ind.part_site AS part_site,ind.u_actual_ship_date AS u_actual_ship_date,ind.u_crd_ship AS u_crd_ship,ind.quantity AS quantity,(CASE WHEN LOWER(ind.STATUS) IN ('historical','shipment')THEN ind.u_orig_quantity ELSE 0 END) AS shippedqty,do.customer_id AS customer_id,do.customer_site As customer_site,ind.STATUS AS status,(CASE WHEN LOWER(ind.STATUS) IN ('historical','shipment')THEN ind.quantity ELSE 0 END) AS closed_quantity,ind.confirmbydate,ind.promiseddate,ind.requestdate,ind.u_acd_mad,ind.u_crd_mad,ind.u_booked_date as u_createdate,ind.u_custorderid,ind.u_matlgr4,ind.u_po_line,ind.u_ponumber,ind.u_profit_center,ind.u_warehouse_source,ind.u_business_unit as u_business_unit,extractString(u_workorder,'_') AS what, ind.u_hl_item,ind.u_userfield_04 as vendorcode,do.u_salesorderuserstatus FROM master_salesitem_maintenance sli JOIN master_planning_product_hierarchy pph on sli.net_element_version_id = pph.net_element_version_id JOIN independentdemand ind ON sli.sales_item_id = ind.part_name JOIN part p ON p.NAME = ind.part_name AND ind.part_site = p.site JOIN referencepart rp ON p.referencepart = rp.value LEFT JOIN u_schedulelinecategory uschline ON ind.u_schedulelinecategory = uschline.id JOIN demandorder do ON ind.order_id = do.id AND ind.order_type = do.type AND ind.order_type_controlset = do.type_controlset WHERE pph.business_unit_id in ('APPLICATION') AND NOT (LOWER(p.type) LIKE 'what%' OR LOWER(p.type) LIKE 'bill%') AND (CASE WHEN ind.u_userfield_05 IS NULL THEN TRUE ELSE (lower(ind.u_userfield_05) NOT LIKE 'iso_exclude%')END) AND (LOWER(ind.order_type) LIKE 'cso%' OR LOWER(ind.order_type) LIKE 'rli' OR LOWER(ind.order_type) LIKE 'shipment%' OR LOWER(ind.order_type) LIKE 'lent%' OR LOWER(ind.order_type) LIKE 'iso%' OR ind.STATUS IN ('historical%','shipment%','shipment%'))AND (LOWER(ind.STATUS) LIKE 'what_comp%' OR NOT(LOWER(ind.STATUS) LIKE 'what%' OR LOWER(ind.STATUS) LIKE 'bill%'))AND (LOWER(uschline.u_schedulelinecategorytype) NOT LIKE 'billable%' OR rp.u_isrlii = TRUE) AND LOWER(ind.STATUS) NOT LIKE 'not_approved' AND LOWER(ind.order_type) NOT LIKE 'sto'").repartition(Constants.NO_OF_PARTITIONS)
			// df.persist(StorageLevel.DISK_ONLY).createOrReplaceTempView("sliorders")
			//df.repartition(20).write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Overwrite).options(Map("cluster" -> "targetCluster", "table" -> "sliorders", "keyspace" -> Constants.CASSANDRA_KEYSPACE)).save()
			status = true
      dfCount = df.count()
      //println("Calculated")
      println("--Completed IND - SLI ORders Action---> " +dfCount)
			df.unpersist();
		} catch {
			case t: Throwable =>
				t.printStackTrace()
				log.error(t.printStackTrace());
        exceptionMsg = "Exception occurred in GenerateSLIOrders_OFUFromInd" +t.getMessage
        }
		(dfCount, status, exceptionMsg)
	}

	def generateSLIOrdersFromHda() : ((Long, Boolean, String)) = {
		println("--Generating SLI Orders HDA")
		var pk = "order_id,line"
		var targetTable = "sliorders"
		var rowstring = "order_id,line,part_name,part_site,customer_id,u_business_unit,customer_site,u_actual_ship_date,u_crd_ship,quantity,shippedqty,status,closed_quantity,requestdate,u_createdate,u_custorderid,u_matlgr4,u_po_line,u_ponumber,u_profit_center,u_warehouse_source,what,u_hl_item,vendorcode"
		var dfCount1: Long = 0L; var status1: Boolean = false; var exceptionMsg1: String = "";
		try {
			val df = sqlContext.sql(s"SELECT hda.u_order AS order_id, hda.u_line AS line, hda.header_partcustomer_part_name AS part_name, hda.header_partcustomer_part_site AS part_site, hda.header_partcustomer_customer_id AS customer_id,hda.u_business_unit as u_business_unit,hda.header_partcustomer_customer_site As customer_site, date_format(cast(unix_timestamp(hda.date, 'MM/dd/yyyy') AS TIMESTAMP), 'yyyy-MM-dd hh:mm:ss') AS u_actual_ship_date, hda.u_crsd AS u_crd_ship, hda.quantity AS quantity, CASE WHEN LOWER( hdc.value ) LIKE 'shipment%' THEN hda.quantity ELSE 0 END AS shippedqty, 'historical' AS status, hda.quantity AS closed_quantity,hda.requestdate as requestdate,hda.u_bookeddate as u_createdate,hda.u_custorderid as u_custorderid,'' as u_matlgr4,'' as u_po_line,''as u_ponumber,hda.u_profit_center as u_profit_center,hda.u_warehousesource as u_warehouse_source,hda.u_hlitem as what, hda.u_hlitem as u_hl_item,'' as vendorcode FROM historicaldemandactual hda, historicaldemandheader hdh, historicaldemandcategory hdc, part p, master_salesitem_maintenance sli JOIN master_planning_product_hierarchy pph on sli.net_element_version_id = pph.net_element_version_id WHERE pph.business_unit_id in ('APPLICATION') AND hda.header_partcustomer_part_name = p.name AND hda.header_partcustomer_part_site = p.site AND LOWER( hda.header_category ) = LOWER( hdh.category ) AND LOWER( hdh.category ) = LOWER( hdc.value ) AND hda.header_partcustomer_part_name = hdh.partcustomer_part_name AND hda.header_partcustomer_part_site = hdh.partcustomer_part_site AND hda.header_partcustomer_customer_id = hdh.partcustomer_customer_id AND hda.header_partcustomer_customer_site = hdh.partcustomer_customer_site AND hda.header_partcustomer_part_name = sli.sales_item_id AND( SELECT COUNT( 1 ) FROM independentdemand ind, part p, site s WHERE ind.part_name = p.name AND ind.part_site = p.site AND p.site = s.value AND LOWER( s.type ) IN( 'distributioncenter2', 'distributioncenter3' ) AND ind.order_id = hda.u_order AND ind.line = hda.u_line GROUP BY ind.order_id, ind.line ) = 0 AND NOT ( LOWER(p.type) LIKE 'what%' OR LOWER(p.type) LIKE 'bill%' ) AND LOWER( hdc.value ) LIKE 'shipment%' AND LOWER( hdc.type ) LIKE 'actual%' AND LOWER( hda.u_outlier ) NOT LIKE 'iso_exclude%'").repartition(Constants.NO_OF_PARTITIONS)
			//df.repartition(20).write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("cluster" -> "targetCluster", "table" -> "sliorders", "keyspace" -> Constants.CASSANDRA_KEYSPACE)).save()
			//insertUpdate(sqlContext, DateTime.now(), targetTable, df, pk, rowstring)
			status1 = true
            dfCount1 = df.count()
            df.show()
			println("--Completed HDA - SLI ORders Action---->" +dfCount1)
			df.unpersist();
		} catch {
			case t: Throwable =>
				t.printStackTrace()
				log.error(t.printStackTrace());
        exceptionMsg1 = "Exception occurred in GenerateSLIOrders_OFUFromInd" + t.getMessage
		}
		
		(dfCount1, status1, exceptionMsg1)
	}
	 def SLIOrdersFromInd():Row={
	 	val startDate: DateTime = DateTime.now()
	 	 val (count, status, exceptionMsg) = generateSLIOrdersFromInd()
	 	 var time = calculateRunTime(startDate, DateTime.now())
		 println("end GenerateSLIOrders IND execute method  ---> " + time)
	 	 val input = Row(Constants.SLI_ORDERS_USING_IND, time, count, status, exceptionMsg, getCurrent_IST_TimeStamp)
         input
	 }
	 def SLIOrdersFromHda():Row={
	 	val startDate: DateTime = DateTime.now()
	 	 val (count, status, exceptionMsg) = generateSLIOrdersFromHda()
	 	 var time = calculateRunTime(startDate, DateTime.now())
		 println("end GenerateSLIOrders IND execute method  ---> " + time)
	 	 val input = Row(Constants.SLI_ORDERS_OFU_HDA, time, count, status, exceptionMsg, getCurrent_IST_TimeStamp)
         input
	 }


	def execute() : Seq[Row] =  {

		val startDate: DateTime = DateTime.now()
		var actionPool: ExecutorService = Executors.newFixedThreadPool(5)
    println("Starting First Actionpool---")
    val action1 = new FutureTask[Unit](new Callable[Unit]() { def call(): Unit = { 
        val a = SLIOrdersFromInd();
        println(a)
		audit += a;
		//println("Success===" +audit)
		//println("Method1--->")
		}
    })
   val action2 = new FutureTask[Unit](new Callable[Unit]() { def call(): Unit = {
		val b = SLIOrdersFromHda();
        println(b)
		audit += b;
		//println("Success===" +audit)
		//println("Method2--->") 
		}
    })
      actionPool.execute(action1)
      actionPool.execute(action2)
      action1.get()
      action2.get()
    
      actionPool.shutdown()
        audit
	}


}