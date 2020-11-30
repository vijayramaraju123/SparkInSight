package org.training

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.joda.time.DateTime

import org.training.CacheTables.cacheTablesInSparkMemory

import org.training.Utilities.{loadResourceToString,getCurrent_IST_TimeStamp, calculateRunTime}

import scala.collection.mutable.ArrayBuffer

class GeneratePLILinkingStageData(sqlContext: SQLContext, sparkContext : SparkContext) {
	@transient lazy val log = org.apache.log4j.LogManager.getLogger(getClass)

	def generatePLILinkingStageData(): ((Long, Boolean, String)) = {
		println("--Writing PLI Linking Stage")
		val LINKING_STAGE: String = loadResourceToString("plilinking_stage")
		var dfCount: Long = 0L; var status: Boolean = false; var exceptionMsg: String = "";
		try {
			val df = sqlContext.sql(LINKING_STAGE).repartition(Constants.NO_OF_PARTITIONS)
			try {
				println("--Starting PLI Linking Stage")
				//df.persist(StorageLevel.DISK_ONLY).createOrReplaceTempView("plilinking_stage")
				df.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Overwrite).options(Map("cluster" -> "targetCluster", "table" -> "plilinking_stage", "keyspace" -> Constants.CASSANDRA_KEYSPACE)).save()
				status = true
				dfCount = df.count
				println("--Completing PLI Linking Stage Action" + dfCount)
				df.unpersist();
			} catch {
				case t: Throwable =>
					t.printStackTrace() // TODO: handle error
					log.error(t.getMessage)
					status = false
					exceptionMsg = "Exception occurred in GeneratePLILinkingStageData" + t.getMessage

			}

		} catch {
			case t: Throwable =>
				t.printStackTrace()
				log.error(t.printStackTrace());
		}
		((dfCount, status, exceptionMsg))
	}

	def execute(): Row = {
	var	tablearr = ArrayBuffer("sliorders", "master_plilinkingrules", "master_itemalias", "itemalias_final")
    cacheTablesInSparkMemory(sparkContext, sqlContext, tablearr, Constants.CASSANDRA_KEYSPACE)
    
		println("starting PLI Linking Stage :")

		val startDate: DateTime = DateTime.now()
		val (count, status, exceptionMsg) = generatePLILinkingStageData()
		val time = calculateRunTime(startDate, DateTime.now())
		println("end PLI Linking Stage execute method  ---> " + time)
		val input = Row(Constants.LINKING_STAGE, time, count, status, exceptionMsg, getCurrent_IST_TimeStamp)
		input
	}
}

