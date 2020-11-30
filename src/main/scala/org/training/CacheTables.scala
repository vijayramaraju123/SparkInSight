package org.training

import java.util.concurrent._

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
 * *  Class will cache the tables from Alluxio to Spark Memory
 *
 */
object CacheTables {
	var alluxio_folderLoc = Constants.ALLUXIO_URL + Constants.ALLUXIO_HOST + ":" + Constants.ALLUXIO_PORT + Constants.ALLUXIO_FOLDER

	def cacheTablesInSparkMemory(sc: SparkContext, sqlContext: SQLContext, tablearr: ArrayBuffer[String], fromKeyspace: String): Unit = {

		val actionPool: ExecutorService = Executors.newFixedThreadPool(8)
		var futures = ArrayBuffer[FutureTask[String]]()

		tablearr.foreach { x =>
			val action1 = new FutureTask[String](new Callable[String]() {
				def call(): String = {
					try {
						sqlContext.read.format("org.apache.spark.sql.cassandra")
						.options(Map("cluster" -> "sourceCluster", "table" -> x, "keyspace" -> fromKeyspace)).load().repartition(Constants.NO_OF_PARTITIONS).persist(StorageLevel.DISK_ONLY).createOrReplaceTempView(x)
						// cacheTableToAlluxio(sc, sqlContext, x)
					} catch {
						case ex: Throwable =>
							{
								println("Cannot cache => " + x)
								ex.printStackTrace()
							}
					}
					x
				}
			})
			actionPool.execute(action1)
			futures += action1
		}

		actionPool.shutdown()

		var done = false
		while (true && !done) {
			try {
				if (actionPool.isTerminated())
					done = true
				if (actionPool.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
					done = true
				}
			} catch {
				case t: Throwable => t.printStackTrace() // TODO: handle error
			}
		}

	}

	def cacheTableToAlluxio(sc: SparkContext, sqlContext: SQLContext, tableName: String) = {

		try {

			val parqfile = sqlContext.read.parquet(alluxio_folderLoc + tableName)
			parqfile.rdd.name = tableName

			var _cachedCount: Long = 0
			tableName match {
				/*case "orderpriority" =>
          parqfile.select("planningpriority", "value", "controlset").coalesce(1).persist(StorageLevel.DISK_ONLY).createOrReplaceTempView(tableName)*/
				case _ =>
					parqfile.repartition(20).persist(StorageLevel.DISK_ONLY).createOrReplaceTempView(tableName)
			}
			_cachedCount = parqfile.count()
			println(tableName + " cached alluxio count = " + _cachedCount)
		} catch {
			case t: Throwable =>
				t.printStackTrace() // TODO: handle error
				println(tableName + "not present in alluxio stopping the process")
				throw t
		}
	}

	def dropTempView(sqlContext: SQLContext, dropTempTableList: ArrayBuffer[String]) {
		for (tableName <- dropTempTableList) {

			scala.util.Try {
				sqlContext.uncacheTable(tableName) //drop TempView
				sqlContext.dropTempTable(tableName)
			}
		}
	}
}