package org.training

import org.apache.spark.SparkContext
import org.apache.spark.sql.cassandra.DataFrameReaderWrapper
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.joda.time.DateTime
import org.training.Utilities.loadResourceToString

import org.training.CacheTables.cacheTablesInSparkMemory
import org.training.Utilities.{calculateRunTime,getCurrent_IST_TimeStamp}


import scala.collection.mutable.ArrayBuffer

class GeneratePLIOrdersData(sqlContext: SQLContext, sparkContext: SparkContext) {
	@transient lazy val log = org.apache.log4j.LogManager.getLogger(getClass)

	def generatePLIOrdersData(): (Long, Boolean, String) = {
		println("--update only PLI information which has linking rules")
		val ORDERS_PLI: String = loadResourceToString("orders_pli")
		var dfCount: Long = 0L; var status: Boolean = false; var exceptionMsg: String = "";
		try {
			val df = sqlContext.sql(ORDERS_PLI).repartition(Constants.NO_OF_PARTITIONS)
			df.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Overwrite).options(Map("cluster" -> "targetCluster", "table" -> "orderspli", "keyspace" -> Constants.CASSANDRA_KEYSPACE)).save()
			// df.persist(StorageLevel.DISK_ONLY).createOrReplaceTempView("orderspli")
			status = true
			dfCount = df.count
			println("1 --Completing generatePLIOrdersData " + dfCount)
			df.unpersist()
		} catch {
			case t: Throwable =>
				t.printStackTrace()
				log.error(t.printStackTrace());
				status = false
				exceptionMsg = "Exception occurred in generatePLIOrdersData" + t.getMessage
		}
		(dfCount, status, exceptionMsg)
	}

	def generateOrdersPLIAllocation(): (Long, Boolean, String) = {
		var dfCount: Long = 0L; var status: Boolean = false; var exceptionMsg: String = "";
		val ORDERS_PLI_ALLOCATION: String = loadResourceToString("orders_pli_allocation")

		try {

			val df = sqlContext.sql(ORDERS_PLI_ALLOCATION).repartition(Constants.NO_OF_PARTITIONS)
			df.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Overwrite).options(Map("cluster" -> "targetCluster", "table" -> "orderspli_allocation", "keyspace" -> Constants.CASSANDRA_KEYSPACE)).save()

			//df.persist(StorageLevel.DISK_ONLY).createOrReplaceTempView("orderspli_allocation")
			status = true
			dfCount = df.count
			println("2 --Completing generateOrdersPLIAllocation : " + dfCount)
			df.unpersist()
		} catch {
			case t: Throwable =>
				t.printStackTrace()
				log.error(t.printStackTrace());
				status = false
				exceptionMsg = "Exception occurred in generatePLIOrdersData" + t.getMessage

		}
		(dfCount, status, exceptionMsg)
	}

	def updateAvlQtyValue(): (Long, Boolean, String) = {
		var dfCount: Long = 0L; var status: Boolean = false; var exceptionMsg: String = "";
		val ORDERS_PLI_ALLOCATION_UPDATE_QTY: String = loadResourceToString("orders_pli_allocation_update_qty")
		/*	var pk = "orderid,pliid,itemid"
		var targetTable = "orderspli_allocation"
		var rowstring = "orderid,pliid,itemid,available_quantity,available_closed_quantity,lineid,plirank,plisequence"
*/
		try {
			val df = sqlContext.sql(ORDERS_PLI_ALLOCATION_UPDATE_QTY).repartition(Constants.NO_OF_PARTITIONS)
			val updateAction = df.rdd.repartition(20).map { x => Row(x.getString(0), x.getString(1), x.getString(2), x.getDouble(3), x.getDouble(4)) }
			val schema = StructType(Array(StructField("orderid", StringType, true), StructField("pliid", StringType, true), StructField("itemid", StringType, true), StructField("available_quantity", DoubleType, true), StructField("available_closed_quantity", DoubleType, true)))
			val updateAvlQuantityAction = sqlContext.applySchema(updateAction, schema)
			updateAvlQuantityAction.show()
			updateAvlQuantityAction.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("cluster" -> "targetCluster", "table" -> "orderspli_allocation", "keyspace" -> Constants.CASSANDRA_KEYSPACE)).save()
			// insertUpdate(sqlContext, DateTime.now(), targetTable, df, pk, rowstring)
			status = true
			dfCount = df.count
			println("3 --Completing ORDERS PLI ALLOCATION UPDATE QTY" + dfCount)
			df.unpersist(); updateAvlQuantityAction.unpersist();
		} catch {
			case t: Throwable =>
				t.printStackTrace()
				log.error(t.printStackTrace());
				status = false
				exceptionMsg = "Exception occurred in generatePLIOrdersData" + t.getMessage

		}
		(dfCount, status, exceptionMsg)
	}

	def updateLineId(): (Long, Boolean, String) = {
		var dfCount: Long = 0L; var status: Boolean = false; var exceptionMsg: String = "";
		val ORDERS_PLI_ALLOCATION_UPDATE_LINE: String = loadResourceToString("orders_pli_allocation_update_line")

		/*		var pk = "orderid,pliid,itemid"
		var targetTable = "orderspli_allocation"
		var rowstring = "orderid,pliid,itemid,lineid,available_quantity,available_closed_quantity,plirank,plisequence"
*/
		try {
			//select oa.orderid, oa.pliid, oa.itemid, max(m1.line_id) as lineid
			val df = sqlContext.sql(ORDERS_PLI_ALLOCATION_UPDATE_LINE).repartition(Constants.NO_OF_PARTITIONS)
			val updateAction = df.rdd.repartition(20).map { x => Row(x.getString(0), x.getString(1), x.getString(2), x.getString(3)) }
			val schema = StructType(Array(StructField("orderid", StringType, true), StructField("pliid", StringType, true), StructField("itemid", StringType, true), StructField("lineid", StringType, true)))
			val updateLineIdAction = sqlContext.applySchema(updateAction, schema)
			updateLineIdAction.show()
			updateLineIdAction.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("cluster" -> "targetCluster", "table" -> "orderspli_allocation", "keyspace" -> Constants.CASSANDRA_KEYSPACE)).save()

			// insertUpdate(sqlContext, DateTime.now(), targetTable, df, pk, rowstring)
			status = true
			dfCount = df.count
			println(" 4 --Completing ORDERS PLI ALLOCATION UPDATE Line" + dfCount)
			df.unpersist(); updateLineIdAction.unpersist();
		} catch {
			case t: Throwable =>
				t.printStackTrace()
				log.error(t.printStackTrace());
				status = false
				exceptionMsg = "Exception occurred in generatePLIOrdersData" + t.getMessage

		}
		(dfCount, status, exceptionMsg)
	}

	def updateRank(): (Long, Boolean, String) = {
		var dfCount: Long = 0L; var status: Boolean = false; var exceptionMsg: String = "";
		val ORDERS_PLI_ALLOCATION_UPDATE_RANK: String = loadResourceToString("orders_pli_allocation_update_rank")
		/*		var pk = "orderid,pliid,itemid"
		var targetTable = "orderspli_allocation"
		var rowstring = "orderid,pliid,itemid,plirank,lineid,available_quantity,available_closed_quantity,plisequence"
*/ try {
			val df = sqlContext.sql(ORDERS_PLI_ALLOCATION_UPDATE_RANK).repartition(Constants.NO_OF_PARTITIONS)
			val updateAction = df.rdd.repartition(20).map { x => Row(x.getString(0), x.getString(1), x.getString(2), x.getInt(3)) }
			val schema = StructType(Array(StructField("orderid", StringType, true), StructField("pliid", StringType, true), StructField("itemid", StringType, true), StructField("plirank", IntegerType, true)))
			val updateRankAction = sqlContext.applySchema(updateAction, schema)
			updateRankAction.show()
			updateRankAction.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("cluster" -> "targetCluster", "table" -> "orderspli_allocation", "keyspace" -> Constants.CASSANDRA_KEYSPACE)).save()
			// insertUpdate(sqlContext, DateTime.now(), targetTable, df, pk, rowstring)
			status = true
			dfCount = df.count
			println("5 --Completing ORDERS PLI ALLOCATION UPDATE Rank" + dfCount)
			df.unpersist(); updateRankAction.unpersist();
		} catch {
			case t: Throwable =>
				t.printStackTrace()
				log.error(t.printStackTrace());
				status = false
				exceptionMsg = "Exception occurred in generatePLIOrdersData" + t.getMessage

		}
		(dfCount, status, exceptionMsg)
	}

	def updatePliQuantity(): (Long, Boolean, String) = {
		val ORDERS_PLI_QTY_UPDATE: String = loadResourceToString("orders_pli_update_pliquantity")
		var dfCount: Long = 0L; var status: Boolean = false; var exceptionMsg: String = "";
		/*		var pk = "lineid,orderid,pliid"
		var targetTable = "orderspli"
		var rowstring = "orderid,pliid,lineid,pliqty,closed_pliqty,u_hl_item"
*/
		loadLinkingTable();
		try {
			val df = sqlContext.sql(ORDERS_PLI_QTY_UPDATE).repartition(Constants.NO_OF_PARTITIONS)
			val updateAction = df.rdd.repartition(20).map { x => Row(x.getString(0), x.getString(1), x.getString(2), x.getFloat(3), x.getFloat(4)) }
			val schema = StructType(Array(StructField("orderid", StringType, true), StructField("pliid", StringType, true),
				StructField("lineid", StringType, true), StructField("pliqty", FloatType, true), StructField("closed_pliqty", FloatType, true)))
			val updatePliQuantityAction = sqlContext.applySchema(updateAction, schema)
			updatePliQuantityAction.show()
			updatePliQuantityAction.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("cluster" -> "targetCluster", "table" -> "orderspli", "keyspace" -> Constants.CASSANDRA_KEYSPACE)).save()
			//insertUpdate(sqlContext, DateTime.now(), targetTable, df, pk, rowstring)
			status = true
			dfCount = df.count
			println("6 --Completing updatePliQuantity" + dfCount)
			df.unpersist(); updatePliQuantityAction.unpersist();
		} catch {
			case t: Throwable =>
				t.printStackTrace()
				log.error(t.printStackTrace());
				status = false
				exceptionMsg = "Exception occurred in updatePliQuantity" + t.getMessage
		}
		(dfCount, status, exceptionMsg)
	}
	def updatePLIandAvlQuantity(): (Long, Boolean, String) = {
		var dfCount: Long = 0L; var status: Boolean = false; var exceptionMsg: String = "";
    loadLinkingTable();

		try {

			val df = sqlContext.sql("select cast(max(plirank) as Integer)as plirank from orderspli_allocation").repartition(Constants.NO_OF_PARTITIONS)
			if (df.select(df("plirank").isNotNull).head().getBoolean(0)) {
				val maxRank = df.collect()(0).getInt(0)
				for (rank <- 1 to maxRank) {
					loadLinkingTable();

					var pliQty = sqlContext.sql("select oa.orderid, oa.pliid, min(rp13.lineid) as lineid, min(oa.available_quantity) as pliqty, min(oa.available_closed_quantity) as closed_pliqty from orderspli_allocation oa left join(select m2.orderid,m2.pliid,min( m2.available_quantity ) as calcqty from orderspli_allocation m2 group by m2.orderid,m2.pliid) rp12 on rp12.orderid = oa.orderid and rp12.pliid = oa.pliid left join( select m3.orderid,m3.pliid,m3.available_quantity,m3.lineid from orderspli_allocation m3) rp13 on rp13.orderid = rp12.orderid and rp13.pliid = rp12.pliid and rp13.available_quantity = rp12.calcqty where oa.plirank = " + rank + " and exists (select 1 from (select pls.order_id, pls.pli from (select oa.orderid, m1.sli from plilinking_stage m1, orderspli_allocation oa where  m1.order_id = oa.orderid and m1.pli = oa.pliid and m1.itemid = oa.itemid and oa.lineid = m1.line_id  group by oa.orderid, m1.sli having count(m1.sli)>1) test, plilinking_stage pls, orderspli op where test.orderid = pls.order_id and test.orderid = op.orderid and test.sli = pls.sli and op.pliid = pls.pli group by pls.order_id, pls.pli) ovlp where ovlp.order_id = oa.orderid and ovlp.pli = oa.pliid) group by oa.orderid,oa.pliid having min(oa.available_quantity) > 0")

					val updateAction = pliQty.rdd.repartition(20).map { x =>
						Row(x.getString(0), x.getString(1), x.getString(2), x.getFloat(3),
							x.getFloat(4))
					}
					val schema = StructType(Array(StructField("orderid", StringType, true), StructField("pliid", StringType, true),
						StructField("lineid", StringType, true),
						StructField("pliqty", FloatType, true),
						StructField("closed_pliqty", FloatType, true)))
					val updatePliQtyAction = sqlContext.applySchema(updateAction, schema)
					println(rank + "--updatePliQtyAction : " + updatePliQtyAction.count)
					updatePliQtyAction.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("cluster" -> "targetCluster", "table" -> "orderspli", "keyspace" -> Constants.CASSANDRA_KEYSPACE)).save()
					pliQty.unpersist(); updatePliQtyAction.unpersist();
					loadLinkingTable();

					var avlQty = sqlContext.sql("select oa.orderid, oa.pliid, oa.itemid, (oa.available_quantity-op.allocatedqty) as available_quantity, (oa.available_closed_quantity-op.allocatedclosedqty) as available_closed_quantity from orderspli_allocation oa, (select op1.orderid, (case when isNullOrEmpty(max(op1.pliqty)) then 0 else max(op1.pliqty) end) as allocatedqty, (case when isNullOrEmpty(max(op1.closed_pliqty)) then 0 else max(op1.closed_pliqty) end) as allocatedclosedqty from orderspli op1, orderspli_allocation oa1 where op1.orderid=oa1.orderid and op1.pliid=oa1.pliid and oa1.plirank= " + rank + " and exists (select 1 from (select pls.order_id, pls.pli from (select oa.orderid, m1.sli from plilinking_stage m1, orderspli_allocation oa where  m1.order_id = oa.orderid and m1.pli = oa.pliid and m1.itemid = oa.itemid and oa.lineid = m1.line_id  group by oa.orderid, m1.sli having count(m1.sli)>1) test, plilinking_stage pls, orderspli op where test.orderid = pls.order_id and test.orderid = op.orderid and test.sli = pls.sli and op.pliid = pls.pli group by pls.order_id, pls.pli) ovlp where ovlp.order_id = oa1.orderid and ovlp.pli = oa1.pliid) group by op1.orderid) op where oa.orderid = op.orderid and exists (select 1 from (select pls.order_id, pls.pli from (select oa.orderid, m1.sli from plilinking_stage m1, orderspli_allocation oa where  m1.order_id = oa.orderid and m1.pli = oa.pliid and m1.itemid = oa.itemid and oa.lineid = m1.line_id  group by oa.orderid, m1.sli having count(m1.sli)>1) test, plilinking_stage pls, orderspli op where test.orderid = pls.order_id and test.orderid = op.orderid and test.sli = pls.sli and op.pliid = pls.pli group by pls.order_id, pls.pli) ovlp where ovlp.order_id = oa.orderid and ovlp.pli = oa.pliid)")

					val updateAction2 = avlQty.rdd.repartition(20).map { x =>
						Row(x.getString(0), x.getString(1), x.getString(2), x.getFloat(3),
							x.getFloat(4))
					}
					val schema2 = StructType(Array(StructField("orderid", StringType, true), StructField("pliid", StringType, true),
						StructField("itemid", StringType, true),
						StructField("available_quantity", FloatType, true),
						StructField("available_closed_quantity", FloatType, true)))
					val updateAvlQtyAction = sqlContext.applySchema(updateAction2, schema2)
					println(rank + "--updateAvlQtyAction : " + updateAvlQtyAction.count)
					updateAvlQtyAction.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("cluster" -> "targetCluster", "table" -> "orderspli_allocation", "keyspace" -> Constants.CASSANDRA_KEYSPACE)).save()
					avlQty.unpersist(); updateAvlQtyAction.unpersist();
				}
			} else {
				println("Max PLI Rank couldn't be found")
			}
			status = true
			println("7 --Completing updatePLIandAvlQuantity" + dfCount)

		} catch {
			case t: Throwable =>
				t.printStackTrace()
				log.error(t.printStackTrace());
				status = false
				exceptionMsg = "Exception occurred in updatePLIandAvlQuantity" + t.getMessage

		}
		(dfCount, status, exceptionMsg)
	}
	def generatePLIOrdersDataUsingPBOM(): (Long, Boolean, String) = {

		println("--Writing Orders PLI table based on the PBOM structure table")
		val ORDERS_PLI_PBOM: String = loadResourceToString("orders_pli_pbom")
		var dfCount: Long = 0L; var status: Boolean = false; var exceptionMsg: String = "";
    loadLinkingTable();

		/*	var pk = "lineid,orderid,pliid"
		var targetTable = "orderspli"
		var rowstring = "orderid,lineid,pliid"*/

		try {
			val pbomDf = sqlContext.sql(ORDERS_PLI_PBOM).repartition(Constants.NO_OF_PARTITIONS)
			val updateAction = pbomDf.rdd.repartition(20).map { x => Row(x.getString(0), x.getString(1), x.getString(2)) }
			val schema = StructType(Array(StructField("orderid", StringType, true), StructField("lineid", StringType, true),
				StructField("pliid", StringType, true)))
			val updatePbomAction = sqlContext.applySchema(updateAction, schema)
			updatePbomAction.show()
			updatePbomAction.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("cluster" -> "targetCluster", "table" -> "orderspli", "keyspace" -> Constants.CASSANDRA_KEYSPACE)).save()
			//      insertUpdate(sqlContext, DateTime.now(), targetTable, pbomDf, pk, rowstring)
			status = true
			dfCount = pbomDf.count
			pbomDf.unpersist(); updatePbomAction.unpersist();
			println("8 --Completing Orders PLI Action using PBOM" + dfCount)
		} catch {
			case t: Throwable =>
				t.printStackTrace()
				log.error(t.printStackTrace());
				status = false
				exceptionMsg = "Exception occurred in generatePLIOrdersDataUsingPBOM" + t.getMessage
		}
		(dfCount, status, exceptionMsg)

	}
	def deleteOrdersPliWithoutPLIAllocation(): (Long, Boolean, String) = {
		loadLinkingTable();
		println("--DELETE the records from OrdersPLI table which does not have PLI allocation but are linked to an order")
		val ORDERS_PLI__WITHOUT_PLI_ALLOCATION: String = loadResourceToString("orders_pli__without_pli_allocation")
		var dfCount: Long = 0L; var status: Boolean = false; var exceptionMsg: String = "";
		try {
			val duplicatesDf = sqlContext.sql(ORDERS_PLI__WITHOUT_PLI_ALLOCATION)
			duplicatesDf.printSchema
			println("--Starting ORDERS_PLI__WITHOUT_PLI_ALLOCATION : " + duplicatesDf.count)
			val ordersPliDf = sqlContext.read.cassandraFormat("orderspli", Constants.CASSANDRA_KEYSPACE).load()
			println("--Starting ORDERS_PLI__WITHOUT_PLI_ALLOCATION : " + ordersPliDf.count)
			ordersPliDf.printSchema
			var exceptDf = ordersPliDf.join(duplicatesDf, Seq("lineid", "orderid", "pliid"), "leftanti")
			exceptDf.printSchema
			exceptDf = exceptDf.na.fill(0.0, Seq("closed_pliqty", "pliqty"))
			dfCount = exceptDf.count
			val updateAction = exceptDf.rdd.repartition(20).map { x =>
				Row(x.getString(0), x.getString(1), x.getString(2), x.getFloat(3),
					x.getTimestamp(4), x.getString(5), x.getTimestamp(6), x.getString(7), x.getFloat(8), x.getString(9))
			}
			val schema = StructType(Array(StructField("lineid", StringType, true), StructField("orderid", StringType, true),
				StructField("pliid", StringType, true),
				StructField("closed_pliqty", FloatType, true), StructField("created_date", TimestampType, true),
				StructField("created_user", StringType, true), StructField("modified_date", TimestampType, true),
				StructField("modified_user", StringType, true), StructField("pliqty", FloatType, true), StructField("u_hl_item", StringType, true)))
			val updatePliWithoutPLIAllocationAction = sqlContext.applySchema(updateAction, schema)
			println("--exceptDf ORDERS_PLI__WITHOUT_PLI_ALLOCATION : " + updatePliWithoutPLIAllocationAction.count)
			updatePliWithoutPLIAllocationAction.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Overwrite).options(Map("cluster" -> "targetCluster", "table" -> "orderspli", "keyspace" -> Constants.CASSANDRA_KEYSPACE)).save()
			status = true
			println(" 9 --Completing ORDERS_PLI__WITHOUT_PLI_ALLOCATION" + dfCount)
			duplicatesDf.unpersist(); exceptDf.unpersist(); updatePliWithoutPLIAllocationAction.unpersist()
		} catch {
			case t: Throwable =>
				t.printStackTrace()
				log.error(t.printStackTrace());
				status = false
				exceptionMsg = "Exception occurred in generatePLIOrdersDataUsingPBOM" + t.getMessage
		}

		(dfCount, status, exceptionMsg)

	}
	def deleteDuplicateOrdersPli(): (Long, Boolean, String) = {
		loadLinkingTable();
		println("--Writing DELETE the duplicate records from OrdersPLI")
		val ORDERS_PLI_DUPLICATES: String = loadResourceToString("orders_pli_duplicates")
		var dfCount: Long = 0L; var status: Boolean = false; var exceptionMsg: String = "";
		try {
			val duplicatesDf = sqlContext.sql(ORDERS_PLI_DUPLICATES)
			duplicatesDf.printSchema
			println("--Starting ORDERS_PLI_DUPLICATES : " + duplicatesDf.count)
			val ordersPliDf = sqlContext.read.cassandraFormat("orderspli", Constants.CASSANDRA_KEYSPACE).load()
			println("--Starting ORDERS_PLI : " + ordersPliDf.count)
			ordersPliDf.show
			ordersPliDf.printSchema
			var exceptDf = ordersPliDf.join(duplicatesDf, Seq("lineid", "orderid", "pliid"), "leftanti")
			exceptDf.printSchema
			exceptDf = exceptDf.na.fill(0.0, Seq("closed_pliqty", "pliqty"))
			dfCount = exceptDf.count
			val updateAction = exceptDf.rdd.repartition(20).map { x =>
				Row(x.getString(0), x.getString(1), x.getString(2), x.getFloat(3),
					x.getTimestamp(4), x.getString(5), x.getTimestamp(6), x.getString(7), x.getFloat(8), x.getString(9))
			}
			val schema = StructType(Array(StructField("lineid", StringType, true), StructField("orderid", StringType, true),
				StructField("pliid", StringType, true),
				StructField("closed_pliqty", FloatType, true), StructField("created_date", TimestampType, true),
				StructField("created_user", StringType, true), StructField("modified_date", TimestampType, true),
				StructField("modified_user", StringType, true), StructField("pliqty", FloatType, true), StructField("u_hl_item", StringType, true)))
			val updatedeleteDuplicateOrdersPliAction = sqlContext.applySchema(updateAction, schema)
			println("--exceptDf ORDERS_PLI : " + updatedeleteDuplicateOrdersPliAction.count)
			updatedeleteDuplicateOrdersPliAction.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Overwrite).options(Map("cluster" -> "targetCluster", "table" -> "orderspli", "keyspace" -> Constants.CASSANDRA_KEYSPACE)).save()
			status = true
			println("10 --Completing deleteDuplicateOrdersPli" + dfCount)
			duplicatesDf.unpersist(); exceptDf.unpersist(); updatedeleteDuplicateOrdersPliAction.unpersist()
		} catch {
			case t: Throwable =>
				t.printStackTrace()
				log.error(t.printStackTrace());
				status = false
				exceptionMsg = "Exception occurred in generatePLIOrdersDataUsingPBOM" + t.getMessage
		}
		(dfCount, status, exceptionMsg)

	}
	def execute(): Seq[Row] = {

		println("starting Orders PLI :")

		var tablearr = ArrayBuffer("master_plilinkingrules", "plilinking_stage", "orderspli", "sliorders", "orderspli_allocation")
		cacheTablesInSparkMemory(sparkContext, sqlContext, tablearr, Constants.CASSANDRA_KEYSPACE)

		val startDate: DateTime = DateTime.now()
		val (count1, status1, exceptionMsg1) = generatePLIOrdersData();
		val (count2, status2, exceptionMsg2) = generateOrdersPLIAllocation();
		val (count3, status3, exceptionMsg3) = updateAvlQtyValue();
		val (count4, status4, exceptionMsg4) = updateLineId();
		val (count5, status5, exceptionMsg5) = updateRank();
		val (count6, status6, exceptionMsg6) = updatePliQuantity();
		val (count7, status7, exceptionMsg7) = updatePLIandAvlQuantity();
		val (count8, status8, exceptionMsg8) = generatePLIOrdersDataUsingPBOM();
		val (count9, status9, exceptionMsg9) = deleteOrdersPliWithoutPLIAllocation();
		val (count10, status10, exceptionMsg10) = deleteDuplicateOrdersPli();

		println("end Orders PLI execute method  ---> " + calculateRunTime(startDate, DateTime.now()))
		val time = calculateRunTime(startDate, DateTime.now())
		println("end Orders PLI execute method  ---> " + time)
		val input = Seq(
			(Row(Constants.ORDERS_PLI, time, count10, status10, exceptionMsg10, getCurrent_IST_TimeStamp)),
			(Row(Constants.ORDERS_PLI_PBOM, time, count8, status8, exceptionMsg8, getCurrent_IST_TimeStamp))
		)
		input
	}
	def loadLinkingTable() = {
		var tablearr = ArrayBuffer("orderspli", "orderspli_allocation")
		cacheTablesInSparkMemory(sparkContext, sqlContext, tablearr, Constants.CASSANDRA_KEYSPACE)
	}
}