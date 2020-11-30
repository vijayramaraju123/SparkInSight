package org.training.spark.xother

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Running join queries on schemaRDD
 */
object Joins {

    def main(args: Array[String]) {
      //Register the table - sales
      val conf = new SparkConf().setMaster(args(0)).setAppName("joins")
      val sc: SparkContext = new SparkContext(conf)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      //val salesDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(args(1))
      val salesDf = sqlContext.read.format("org.apache.spark.sql.json").option("header", "true").load(args(1))
      salesDf.registerTempTable("sales")
      sqlContext.cacheTable("sales")

      //Register the table customer

      //val customerDf = sqlContext.read.format("org.apache.spark.sql.json").load(args(2))
      val customerDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(args(2))
      customerDf.registerTempTable("customer")


      joinQuery(sqlContext)
      nestedQuery(sqlContext)
    }

   def joinQuery(sqlcontext:SQLContext){
    //Join two tables
    val customerJoinSales = sqlcontext.sql("select * from sales join customer  on (customer.customerId=sales.customerId)")
     customerJoinSales.show()
  }

  def nestedQuery(sqlcontext:SQLContext){
    //Nested query
    val nestedOutput = sqlcontext.sql("select customerId,total_amount from ( select customerId, sum(amountPaid) total_amount from sales group by customerId ) t2 where t2.total_amount >=600")
    nestedOutput.show()

    nestedOutput.registerTempTable("HighSalesCustomer")
    val customerJoinHighSales = sqlcontext.sql("select * from  HighSalesCustomer join customer  on (customer.customerId=HighSalesCustomer.customerId)")
    customerJoinHighSales.show()
  }
}
