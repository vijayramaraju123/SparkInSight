package org.training.spark.batch.sql

import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object Class13RdbmsIntegrationApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Class11RdbmsIntegrationApp")
      .master("local")
      .getOrCreate()

    /*
      In order to connect to any RDBMS we need below parameters.

      URL : The JDBC URL to connect to, The source-specific connection properties may be specified in the URL

      dbtable : The JDBC table that should be read from or written into.
                Note that when using it in the read path anything that is valid in a FROM clause of a SQL query can be used.
                For example, instead of a full table you could also use a subquery in parentheses.
                It is not allowed to specify `dbtable` and `query` options at the same time.

      user : user of database

      password : password of the user

      fetchSize : The JDBC fetch size, which determines how many rows to fetch per round trip.
                  This can help performance on JDBC drivers which default to low fetch size (eg. Oracle with 10 rows).
                  This option applies only to reading.

      partitionColumn,lowerBound,upperBound:

        They describe how to partition the table when reading in parallel from multiple workers.
        partitionColumn must be a numeric, date, or timestamp column from the table in question.
        Notice that lowerBound and upperBound are just used to decide the partition stride, not for filtering the rows in table.
        So all rows in the table will be partitioned and returned. This option applies only to reading.
        In addition, numPartitions must be specified.

     numPartitions : The maximum number of partitions that can be used for parallelism in table reading and writing.
        This also determines the maximum number of concurrent JDBC connections.
        If the number of partitions to write exceeds this limit, we decrease it to this limit by calling coalesce(numPartitions) before writing.
     */

    //login command for mysql : mysql -u root -p

    //Approach 1 :

    val jdbcOptions = Map("url" -> "jdbc:mysql://localhost:3306/ecommerce",
                          "dbtable" -> "sales",
                          "user" -> "root",
                          "password" -> "cloudera",
                          "fetchSize" -> "10")

    val jdbcDF = spark.read
                  .format("jdbc")
                  .options(jdbcOptions)
                  .load()

    //jdbcDF.show()

    // Approach 2 : Using jdbc method

    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","cloudera")

    val jdbcDF1 = spark.read.jdbc("jdbc:mysql://localhost:3306/ecommerce","kafka_sales",props)

    jdbcDF1//.show()

    // Approach 3: Parallel read from RDBMS Tables

    val parallelReadOpts = Map("url" -> "jdbc:mysql://localhost:3306/ecommerce",
                          "dbtable" -> "sales",
                          "user" -> "root",
                          "password" -> "cloudera",
                          "fetchSize" -> "10",
                          "partitionColumn" -> "customerId",
                          "lowerBound" -> "1",
                          "upperBound" -> "10",
                          "numPartitions" -> "2")

    val parallelJdbcDF = spark.read.format("jdbc").options(parallelReadOpts).load()

    parallelJdbcDF//.show()

    // Predicate push down queries

    val query = "(select * from sales where amountPaid > 1000) tmp"

    val predicateDF = spark.read.jdbc("jdbc:mysql://localhost:3306/ecommerce",query,props)

    predicateDF//.show()

    jdbcDF.filter("amountPaid > 1000")//.show()


    //Reading data from files(local/HDFS/cloud) and Writing data into RDBMS

    val csvSalesDf = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(args(0))
      .withColumn("date",current_date())

    //csvSalesDf.write.jdbc("jdbc:mysql://localhost:3306/ecommerce","sales_csv",props)

    //updating data in date filed and overwriting existing table
    val newSalesDF = csvSalesDf.withColumn("date",date_add(current_date,10))

    //newSalesDF.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/ecommerce","sales_csv",props)
    //newSalesDF.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/ecommerce","sales_csv",props)

    // Updating existing records in RDBMS Tables

    val updatedAmountDf = newSalesDF
      .selectExpr("transactionId","customerId","itemId","amountPaid*2 amountPaid")
      //.filter("amountPaid>2000")

    updatedAmountDf
      .repartition(10)
      .foreachPartition(part => {

      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/ecommerce","root","cloudera")

          part.foreach(row => {

            //val id = row.getInt(0)
            //val amount = row.getDouble(3)

            val id = row.getAs[Int]("transactionId")
            val amount = row.getAs[Double]("amountPaid")

            val sql = s"update sales_csv set amountPaid=$amount where transactionId=$id"
            //val sql = s"delete from sales_csv where transactionId=$id and amountPaid>2000"
            println(sql)
            val sqlStmt = conn.prepareStatement(sql)
            sqlStmt.execute()
        })

    })

    Thread.sleep(100000)
  }

}
