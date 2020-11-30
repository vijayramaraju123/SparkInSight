package org.training.spark.oriellyFlightData

import java.text.SimpleDateFormat
import java.util.Date
import java.util.logging.Logger

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{array_contains, col, current_date, current_timestamp, date_add, date_sub, datediff, explode, expr, initcap, lit, lower, lpad, ltrim, map, months_between, not, regexp_extract, regexp_replace, rpad, rtrim, split, to_date, translate, trim, unix_timestamp, upper}
import org.training.spark.customerLoadCredit.CustomerLoadCreditApp.getClass
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object FlightData {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(getClass.getName)
      .getOrCreate()

    var format=new SimpleDateFormat("dd-MM-YYYY-HH-mm-ss")
    System.setProperty("current.date.time",format.format(new Date()))

val logger:Logger=Logger.getLogger(getClass.getName)


    val optMaps = Map("header" -> "true", "inferSchema" -> "true", "delimiter" -> ",")

    val flightData2015 = spark.read
      .format("csv")
      .options(optMaps)
      .load("C:\\Users\\Incredible\\Documents\\run\\datasets-master\\sdg_datasets\\flight-data\\csv\\2015-summary.csv").cache()

    flightData2015.createOrReplaceTempView("flight_data_2015")



    //*******************************//

    val maxdsl=flightData2015.groupBy("DEST_COUNTRY_NAME")
      .agg(sum("count").as("totalDestination")).show()

    //*******************************//

    val sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
""").show()

    val dataFrameWay = flightData2015
      .groupBy(col("DEST_COUNTRY_NAME"))
      .count().show()

    flightData2015.select(max("count")).take(1).foreach(println)
    spark.sql("SELECT max(count) from flight_data_2015").take(1).foreach(println)

//*******************************//

    val maxSql = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
""").show()

    flightData2015
      .groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)", "destination_total")
      .sort(desc("destination_total"))
      .limit(5)
      .show()

    //*******************************//

    case class Flight(DEST_COUNTRY_NAME: String,
                      ORIGIN_COUNTRY_NAME: String,
                      count: BigInt)
    import spark.implicits._

/*    val flightsDF = spark.read
      .parquet("C:\\Users\\Incredible\\Documents\\run\\datasets-master\\sdg_datasets\\flight-data\\parquet\\2010-summary.parquet")
    val flights = flightsDF.as[Flight]*/

    //*******************************//

val dfr=spark.range(10).toDF("numbers")
    dfr.select(col("numbers")).show()

    val myRow = Row("Hello", null, 1, false)

    myRow(0) // type Any
    myRow(0).asInstanceOf[String] // String
    myRow.getString(0) // String
    myRow.getInt(2) // Int


    val myManualSchema = new StructType(Array(
      new StructField("some", StringType, true),
      new StructField("col", StringType, true),
      new StructField("names", LongType, false)))
    val myRows = Seq(Row("Hello", null, 1L))
    val myRDD = spark.sparkContext.parallelize(myRows)
    val myDf = spark.createDataFrame(myRDD, myManualSchema)
    myDf.show()

    flightData2015.select(expr("DEST_COUNTRY_NAME as destination")).show()
    flightData2015.select(expr("DEST_COUNTRY_NAME as destination ").alias("DESTCONTRY")).show()

    flightData2015.selectExpr("DEST_COUNTRY_NAME as desticountry","DEST_COUNTRY_NAME").show()

    flightData2015.selectExpr("*","(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as sameinReg").show()


      flightData2015.select(expr("*"), lit(1).as("One")).show(2)
    flightData2015.select(expr("*"), lit(24).as("one")).show()

    flightData2015.withColumn("NewColumn",lit(240))

    flightData2015.withColumn("newCol", expr("DEST_COUNTRY_NAME == ORIGIN_COUNTRY_NAME")).show()
    flightData2015.withColumn("addCont", col("DEST_COUNTRY_NAME")===col("ORIGIN_COUNTRY_NAME")).show()
    flightData2015.withColumn("Name", expr("DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME")).show()

    flightData2015.withColumn("casting", col("ORIGIN_COUNTRY_NAME").cast("long")).printSchema()

    flightData2015.withColumn("casting", col("ORIGIN_COUNTRY_NAME").cast("long")).drop("ORIGIN_COUNTRY_NAME").show()

    flightData2015.filter(col("count") < 2 ).show()
    flightData2015.where(col("count") < 2 ).show()

    flightData2015.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") =!= "croatia")

    flightData2015.withColumn("test",col("ORIGIN_COUNTRY_NAME")=== col("DEST_COUNTRY_NAME")).count()

      flightData2015.where(col("ORIGIN_COUNTRY_NAME").equalTo("USA")).show()

    flightData2015.select("ORIGIN_COUNTRY_NAME").distinct().count()

    flightData2015.sort("count").show(5)
    flightData2015.orderBy("count", "DEST_COUNTRY_NAME").show(5)
    flightData2015.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)


    flightData2015.orderBy(expr("count desc")).show(2)
    flightData2015.orderBy(desc("count"), asc("DEST_COUNTRY_NAME")).show(2)

    flightData2015.orderBy(desc("count"),asc("ORIGIN_COUNTRY_NAME")).show()

 flightData2015.limit(5).show()

    println(flightData2015.rdd.getNumPartitions)

    println(flightData2015.repartition(6).rdd.getNumPartitions)

    flightData2015.repartition(6, col("ORIGIN_COUNTRY_NAME"))

    flightData2015.describe("*")

    flightData2015.repartition(6).coalesce(4)

    val collectDF = flightData2015.limit(10)
    collectDF.take(5) // take works with an Integer count
    collectDF.show() // this prints it out nicely
    collectDF.show(5, false)
    collectDF.collect()






    val df1 = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("C:\\Users\\Incredible\\Documents\\run\\datasets-master\\sdg_datasets\\retail-data\\by-day\\2010-12-01.csv").cache()



    df1.where(col("InvoiceNo") === 536365)
      .select("InvoiceNo", "Description")
      .show(5, false)


    df1.where("InvoiceNo = 536365")
      .show(5, false)



    val DOTCodeFilter = col("StockCode") === "DOT"
    val priceFilter = col("UnitPrice") > 600
    val descripFilter = col("Description").contains("POSTAGE")
    df1.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descripFilter)))
   //   .where("isExpensive")
      .select("unitPrice", "isExpensive").show(5)


    df1.withColumn("isExpensive", not(col("UnitPrice").leq(250)))
      .filter("isExpensive")
      .select("Description", "UnitPrice").show(5)

    df1.withColumn("isExpensive", not(col("UnitPrice").leq(250)))
      .where("isExpensive")
      .select("Description", "UnitPrice").show(5)

    df1.select(col("Description"),
      lower(col("Description")),
      upper(lower(col("Description")))).show(2)


    val schema1 = myDf.schema
    val newRows = Seq(
      Row("New Country", "Other Country", 5L),
      Row("New Country 2", "Other Country 3", 1L)
    )

    val parallelizedRows = spark.sparkContext.parallelize(newRows)
    val newDF = spark.createDataFrame(parallelizedRows, schema1)

   /* myDf.union(newDF).show()
    myDf.union(newDF)
      .where("count = 1")
      .where($"ORIGIN_COUNTRY_NAME" =!= "United States")
      .show() // get all of them and we'll see our new rows at the end
*/





    /*
//actions on dataframe
  flightData2015.show()
  flightData2015.take(5).foreach(println)
  flightData2015.count()
*/
    // flightData2015.collect().foreach(println)

    /*

    // sorting the data
    val dataFrameWay3 = flightData2015.groupBy("DEST_COUNTRY_NAME").count().show()
    val dataFrameWay4 = flightData2015.sort(asc("DEST_COUNTRY_NAME")).groupBy("DEST_COUNTRY_NAME").count().show()
    val dataFrameWay = flightData2015.sort(desc("DEST_COUNTRY_NAME")).groupBy("DEST_COUNTRY_NAME").count().show()
    val dataFrameWay1 = flightData2015.select("*").orderBy(desc("DEST_COUNTRY_NAME")).groupBy("DEST_COUNTRY_NAME").agg(sum("count").as("SUM")).show()
    flightData2015.select(min("count")).take(10).foreach(println)

  */


    /*
   flightData2015.createOrReplaceTempView("dfTable")

   flightData2015
     .groupBy("DEST_COUNTRY_NAME")
     .sum("count")
     .withColumnRenamed("sum(count)", "destination_total")
     .sort(desc("destination_total"))
     .limit(5).show()

   flightData2015.groupBy("DEST_COUNTRY_NAME")
     .sum("count")
     .withColumnRenamed("sum(count)", "totalSum")
     .sort(desc("totalSum"))
     .limit(5).show()

   flightData2015.groupBy("DEST_COUNTRY_NAME").agg(sum("count"))
     .withColumnRenamed("sum(count)","totalsum")
     .sort(desc("totalsum"))
     .limit(5).explain()

   val DF1=flightData2015
   val DF2 = DF1.groupBy("DEST_COUNTRY_NAME").count().collect().foreach(println)

   DF1.select("DEST_COUNTRY_NAME").show(2)

   DF1.select(
     "DEST_COUNTRY_NAME",
     "ORIGIN_COUNTRY_NAME")
     .show(2)

   import spark.implicits._

   DF1.select(
     DF1.col("DEST_COUNTRY_NAME"),
     col("DEST_COUNTRY_NAME"),
     column("DEST_COUNTRY_NAME"),
     'DEST_COUNTRY_NAME , $"DEST_COUNTRY_NAME",
     expr("DEST_COUNTRY_NAME")
   ).show(2)

   DF1.select(
     DF1.col("DEST_COUNTRY_NAME"),
     column("DEST_COUNTRY_NAME"),
       col("DEST_COUNTRY_NAME"),
     $"DEST_COUNTRY_NAME",
     'DEST_COUNTRY_NAME
   ).show()

   DF1.selectExpr("*", "DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)
   DF1.select(expr("DEST_COUNTRY_NAME"). alias("DestinationCntry")).show()
 DF1.select(expr("DEST_COUNTRY_NAME as destCntryName")).show()
   DF1.select(col("DEST_COUNTRY_NAME"). alias("DestinationCntry")).show()
   DF1.selectExpr("avg(DEST_COUNTRY_NAME) as avgCntry","count(DEST_COUNTRY_NAME) as countCntry").show()

   DF1.select(expr("*"), lit(1).as("something")).show(2)
   DF1.withColumn("numberOne", lit(1)).show(2)

   DF1.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME")).show(2)

   DF1.withColumn("withinCountry", when(col("ORIGIN_COUNTRY_NAME") === ("DEST_COUNTRY_NAME"),"yes")
       .otherwise("No")).show()

   DF1.withColumn("Destination", DF1.col("DEST_COUNTRY_NAME")).show()

   val dfWithLongColName = DF1.withColumn("ThisLongColumn-Name", expr("ORIGIN_COUNTRY_NAME"))
  // dfWithLongColName.selectExpr("ThisLongColumn-Name, ThisLongColumn-Name as new-col").show(2)
  dfWithLongColName.select(col("ThisLongColumn-Name")).show()

   DF1.printSchema()
   DF1.withColumn("count", col("count").cast("int")).printSchema()
 DF1.withColumn("count",col("count").cast("double"))

 val colCondition=DF1.filter(col("count") < 5).take(2)
 DF1.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME").isin("United States")).show()
   DF1.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia").show(2)
   DF1.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") === "Croatia").show(2)

   DF1.sort("count").show(52)
   DF1.orderBy("count", "DEST_COUNTRY_NAME").show(35)
   DF1.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(50)


   DF1.orderBy(expr("count desc")).show()
   DF1.orderBy(desc("count"), asc("DEST_COUNTRY_NAME")).show()

 DF1.repartition(5,col("count")).coalesce(2)

  */


    /*
    val df1 = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("C:\\Users\\Incredible\\Documents\\run\\datasets-master\\sdg_datasets\\retail-data\\by-day\\2010-12-01.csv").cache()


    df1.show()
    df1.printSchema()

    df1.where(col("InvoiceNo").equalTo(536365))
      .select("InvoiceNo", "Description").show(5, false)

    df1.where(col("InvoiceNo").equalTo(536365))
      .select("InvoiceNo", "Description").show()

    df1.where(col("InvoiceNo") === 536365)
      .select("InvoiceNo", "Description").limit(5).show()

    val DOTCodeFilter = col("StockCode") === "DOT"
    val priceFilter = col("UnitPrice") > 600
    val descripFilter = col("Description").contains("POSTAGE")

*/


    /* df1.withColumn("isExpensive", DOTCodeFilter.and(col("priceFilter or descripFilter")))
     .where("isExpensive")
     .select("unitPrice", "isExpensive")
     .show(5)
 */

 /*
    val priceFilter1 = col("UnitPrice") > 600
    val descripFilter1 = col("Description").contains("POSTAGE")
    df1.where(col("StockCode").isin("DOT"))
      .where(priceFilter1.and(descripFilter1))
      .show(5)

    df1.where(col("StockCode").isin("DOT"))
      .where(priceFilter1.or(descripFilter1))
      .show(5)


    df1.where(col("StockCode").isin("DOT"))
      .where(col("UnitPrice") > 600).where(col("Description").contains("POSTAGE"))
      .show(5)


    val DOTCodeFilter1 = col("StockCode") === "DOT"
    val priceFilter2 = col("UnitPrice") > 600
    val descripFilter2 = col("Description").contains("POSTAGE")

*/


    /*  df1.withColumn("isExpensive", DOTCodeFilter1.and(col("priceFilter2 or descripFilter2")))
      .where("isExpensive")
      .select("unitPrice", "isExpensive")
      .show(5)*/

    //  Boolean expressions are not just reserved to filters. In order to filter a DataFrame we can also just specify a boolean column.

/*

    df1.withColumn("isExpensive",
      DOTCodeFilter.and(priceFilter.or(descripFilter)))
      .where("isExpensive") // it yeilds boolean of dataframe
      .select("unitPrice", "isExpensive")
      .show(5)

    df1.withColumn("isExpensive", col("UnitPrice").leq(250))
      .filter("isExpensive")
      .select("Description", "UnitPrice")
      .show(5)

    df1.withColumn("isExpensive", not(col("UnitPrice").leq(250)))
      .filter("isExpensive")
      .select("Description", "UnitPrice")
      .show(5)


    df1.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descripFilter)))
      .where("isExpensive")
      .select("unitPrice", "isExpensive")
      .show(5)

    df1.withColumn("isExpensive",
      DOTCodeFilter.and(priceFilter.or(descripFilter)))
      .where("isExpensive")
      .select("unitPrice", "isExpensive").show(5)

    df1.withColumn("isExpensive", not(col("UnitPrice").leq(250)))
      .filter("isExpensive")
      .select("Description", "UnitPrice").show(5)

    df1.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))
      .filter("isExpensive")
      .select("Description", "UnitPrice").show(5)

    df1.select(
      col("Description"),
      lower(col("Description")),
      upper(lower(col("Description"))))
      .show(2)

    val dateDF = spark.range(10)
      .withColumn("today", current_date())
      .withColumn("now", current_timestamp())

    dateDF.createOrReplaceTempView("dateTable")

    dateDF.show()

    dateDF
      .select(
        date_sub(col("today"), 5),
        date_add(col("today"), 5))
      .show(1)

    dateDF
      .withColumn("week_ago", date_sub(col("today"), 7))
      .select(datediff(col("week_ago"), col("today")))
      .show()

    dateDF
      .select(to_date(lit("2016-01-01")).alias("start"),
        to_date(lit("2017-05-22")).alias("end"))
      .select(months_between(col("start"), col("end")))
      .show(1)

    dateDF
      .select(current_date().alias("start"),
        to_date(lit("2023-05-22")).alias("end"))
      .select(months_between(col("start"), col("end")))
      .show(1)

    spark.range(5).withColumn("date", lit("2017-01-01"))
      .select(to_date(col("date")))
      .show()

    dateDF.select(to_date(lit("2016-20-12")),
      to_date(lit("2017-12-11")))
      .show(1)

    val dateFormat = "yyyy-dd-MM"
    val cleanDateDF = spark.range(1)
      .select(
        to_date(unix_timestamp(lit("2017-12-11"), dateFormat).cast("timestamp")).alias("date"),
        to_date(unix_timestamp(lit("2017-20-12"), dateFormat).cast("timestamp"))
          .alias("date2"))

    cleanDateDF.
      select(unix_timestamp(col("date"), dateFormat).cast("timestamp"))
      .show()

    cleanDateDF.filter(col("date2") > lit("2017-12-12")).show()


    df1.na.drop()
    df1.na.drop("any")

    df1.na.drop("all")

    df1.na.drop("all", Seq("StockCode", "InvoiceNo")).show()

    df1.na.fill("All Null values become this string").show()

    df1.na.fill(5, Seq("StockCode", "InvoiceNo"))

    val fillColValues = Map(
      "StockCode" -> 5,
      "Description" -> "No Value")

    df1.na.fill(fillColValues)

    df1.na.replace("Description", Map("" -> "UNKNOWN"))

    df1.select(split(col("Description"), " ")).show(2)

    df1.select(split(col("Description"), " ").alias("array_col"))
      .selectExpr("array_col[0]")
      .show(2)

    df1.select(array_contains(split(col("Description"), " "), "WHITE")).show(2)

    df1.withColumn("splitted", split(col("Description"), " "))
      .withColumn("exploded", explode(col("splitted")))
      .select("Description", "InvoiceNo", "exploded").show()

    df1.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
      .selectExpr("complex_map['Description']").show()

    df1.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
      .selectExpr("complex_map").show(5, false)

    df1.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
      .selectExpr("explode(complex_map)")
      .show(5, false)


    df1.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
      //   .selectExpr("explode(complex_map)")
      .show(5, false)

    df1.select(initcap(col("Description"))).show(2, false)

    df1.select(
      col("Description"),
      lower(col("Description")),
      upper(lower(col("Description"))))
      .show(2)

    df1.select(
      ltrim(lit(" HELLO ")).as("ltrim"),
      rtrim(lit(" HELLO ")).as("rtrim"),
      trim(lit(" HELLO ")).as("trim"),
      lpad(lit("HELLO"), 3, " ").as("lp"),
      rpad(lit("HELLO"), 10, " ").as("rp"),
      lpad(lit(" HELLO "), 8, " ").as("pic"))
      .show(2, false)

    val simpleColors = Seq("black", "white", "red", "green", "blue")
    val regexString = simpleColors.map(_.toUpperCase).mkString("|")

    df1.select(
      regexp_replace(col("Description"), regexString, "COLOR")
        .alias("color_cleaned"),
      col("Description"))
      .show(2)

    df1.select(
      translate(col("Description"), "LEET", "1337"),
      col("Description"))
      .show(2)

    val regexString1 = simpleColors
      .map(_.toUpperCase)
      .mkString("(", "|", ")")

    df1.select(
      regexp_extract(col("Description"), regexString1, 1)
        .alias("color_cleaned"),
      col("Description"))
      .show(2)

    val containsBlack = col("Description").contains("BLACK")
    val containsWhite = col("DESCRIPTION").contains("WHITE")
    df1.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
      .filter("hasSimpleColor")
      .select("Description")
      .show(3, false)

    val simpleColors2 = Seq("black", "white", "red", "green", "blue")
    val selectedColumns = simpleColors2.map(color => {
      col("Description")
        .contains(color.toUpperCase)
        .alias(s"is_$color")
    }) :+ expr("*") // could also append this value

*/

  }

}