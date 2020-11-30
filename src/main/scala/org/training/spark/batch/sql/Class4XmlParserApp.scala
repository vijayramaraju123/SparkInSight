package org.training.spark.batch.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Class4XmlParserApp extends App {

  val spark = SparkSession.builder()
    .appName(getClass.getName)
    .master("local")
    .getOrCreate()

  val xmlDF = spark.read
    .format("xml")
    .option("rowTag", "person")
    .load(args(1))

  xmlDF.printSchema()
  xmlDF.show()


  val df1 = xmlDF.select("name","age._VALUE","age._born")

  df1.show
  df1.printSchema()

  //Aliasing columns
  import spark.implicits._

  xmlDF.selectExpr("name","age._VALUE as age","age._born birth_date")
    .show()

  val agesDf = xmlDF.select(xmlDF("name"),$"age._VALUE".alias("age"),col("age._born").as("birth_date"))

  agesDf.write.format("xml").option("rowTag","person").save("src/main/resources/output/xml")


//  val xmlArrayDF = spark.read.format("xml")
//    .option("rowTag", "people")
//    .load(args(1))
//
//  xmlArrayDF.printSchema()
//  xmlArrayDF.show(false)
//
//  val df2 = xmlArrayDF.select(explode(col("person")).as("person"))
//
//  df2.show()
//
//  val xmlOutptDf = df2.selectExpr("person.name","person.age._birthplace birth_date","person.age._VALUE age")
//
//  xmlOutptDf.write.format("xml").option("rowTag","person").save("src/main/resources/output/xml")


}
