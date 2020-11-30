package org.training.spark.batch.sql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


object Class1CsvParserApp {

  def main(args: Array[String]) {

    /*
      In spark 1.x we will follow below approach to read CSV
     */

    val sparkConf = new SparkConf().setMaster(args(0)).setAppName("Class1CsvParserApp")
    val sc  = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val hiveCtx = new HiveContext(sc)

    /*
    Options for csv format
    -->header: (Column Names)
          By default this option is set to false.This will use the first row in the csv file as the dataframe's column names.
          If you don't set to true even if header is present in the file then header will also be considered as row and
          default column names will be set _c0, _c1, _c2, etc.
    -->inferSchema: (Column Type)
        The schema referred here are the column types. A column can be of type String, Double, Long, etc.
        Using inferSchema=false (default option) will give a dataframe where all columns are strings (StringType)
        By setting inferSchema=true, Spark will automatically go through the csv file and infer the schema of each column
    -->delimiter:
        Delimiter used in the csv file to seperate the fields.default value is ,(Comma) for csv file
    -->mode:
        PERMISSIVE: try to parse all lines: nulls are inserted for missing tokens and extra tokens are ignored.
        DROPMALFORMED: drop lines that have fewer or more tokens than expected or tokens which do not match the schema.
        FAILFAST: abort with a RuntimeException if any malformed line is encountered.
     */

    //Approach 1

    //Passing all options with Options methos using Map object

    val optMaps = Map("header" -> "true", "inferSchema" -> "true", "delimiter" -> ",")

    val salesDF = sqlContext.read
                          .format("csv")
                          .options(optMaps)
                          .load(args(1))

//    salesDF.printSchema()
//    salesDF.show()

    //Approach 2

    //Using option for passing one option at a time

    val salesDF2 = sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(args(1))

//    salesDF2.printSchema()
//    salesDF2.show()

    //Approach 3

    /*
      StructType objects define the schema of Spark DataFrames.
      StructType objects contain a list of StructField objects that define the name, type, and nullable flag for each column in a DataFrame.
        name => Column Name
        type => Column Type it can be one of the type based on the value DoubleType,IntegerType,StringType,TimestampType,ect
        nullable => values are true or false. Set to true if you allow nulls value for a column
     */

//    val schema = StructType(
//      Array(
//        StructField("transactionId", IntegerType, true),
//        StructField("customerId", IntegerType, true),
//        StructField("itemId", IntegerType, true),
//        StructField("amountPaid", DoubleType, true)
//      ))

    Long

    val salesSchema = StructType(Array(
      StructField("transactionId",LongType,true),
      StructField("customerId",LongType,true),
      StructField("itemId",IntegerType,false),
      StructField("amountPaid",DoubleType,true)))

    val salesDF4 = sqlContext.read.format("csv")
      .option("header", "true")
      //.option("inferSchema", "true") don't set this option
      .option("delimiter", ",")
      .schema(salesSchema)
      .load(args(1))

    salesDF4.printSchema()
    salesDF4.show()

    Thread.sleep(100000)

  }

}
