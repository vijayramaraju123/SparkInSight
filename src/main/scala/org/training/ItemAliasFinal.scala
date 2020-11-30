package org.training

import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.joda.time.DateTime
import org.training.Utilities.calculateRunTime
import org.training.Utilities.getCurrent_IST_TimeStamp
import org.training.Utilities.loadResourceToString

class ItemAliasFinal(sqlContext: SQLContext) {

  @transient lazy val log = org.apache.log4j.LogManager.getLogger(getClass)

	def generateFinalItemAlias(): ((Long, Boolean, String)) = {
		println("--Writing Item Alias Final ")
		val ITEM_ALIAS_FINAL: String = loadResourceToString("itemalias_final")
		var dfCount: Long = 0L; var status: Boolean = false; var exceptionMsg: String = "";
		try {
			val item_df = sqlContext.sql("select sales_item_id,itemaliasid from master_itemalias").repartition(Constants.NO_OF_PARTITIONS)
			val sli_item_df = sqlContext.sql(ITEM_ALIAS_FINAL).repartition(Constants.NO_OF_PARTITIONS)
			try {
				println("--Starting ITEM_ALIAS_FINAL" + sli_item_df.count())
				val total_df = item_df.union(sli_item_df)
				total_df.show
				//total_df.persist(StorageLevel.DISK_ONLY).createOrReplaceTempView("itemalias_final")
				total_df.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Overwrite).options(Map("cluster" -> "targetCluster", "table" -> "itemalias_final", "keyspace" -> Constants.CASSANDRA_KEYSPACE)).save()
				status = true
				dfCount = total_df.count
				println("--Completed generateFinalItemAlias" + dfCount)
				total_df.unpersist(); item_df.unpersist(); sli_item_df.unpersist();
			} catch {
				case t: Throwable =>
					t.printStackTrace() // TODO: handle error
					log.error(t.getMessage)
					status = false
					exceptionMsg = "Exception occurred in generateFinalItemAlias" + t.getMessage

			}
			println("--Completing ITEM_ALIAS_FINAL")

		} catch {
			case t: Throwable =>
				t.printStackTrace()
				log.error(t.printStackTrace());
		}
		((dfCount, status, exceptionMsg))
	}

  def execute(): Row = {
    println("starting Item Alias Final :")
    val startDate: DateTime = DateTime.now()
    val (count, status, exceptionMsg) = generateFinalItemAlias();
    val time = calculateRunTime(startDate, DateTime.now())
    println("end Item Alias Final execute method  ---> " + time)
    val input = Row(Constants.ITEM_ALIAS_FINAL, time, count, status, exceptionMsg, getCurrent_IST_TimeStamp)
    input
  }

}