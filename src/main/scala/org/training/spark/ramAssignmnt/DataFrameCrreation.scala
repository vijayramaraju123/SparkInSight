package org.training.spark.ramAssignmnt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructType}

object DataFrameCrreation {
  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder()
      .master("local[*]")
      .appName(getClass.getName)
      .getOrCreate()

    val df1=spark.read.format("json")
      .option("multiLine", true)
      .load("C:\\Users\\Incredible\\Documents\\run\\ramAssignmnt\\Dataframe.json")
     df1.show(4, false)

    // to read column names and data types
    val (columnNames, columnDataTypes) = df1.dtypes.unzip

    println(columnNames)
    println((columnDataTypes))

// explode option
    import spark.implicits._

    val df = df1.select(explode($"stackoverflow") as "stackoverflow_tags")

    df.select(
      $"stackoverflow_tags.tag.id" as "id",
      $"stackoverflow_tags.tag.author" as "author",
      $"stackoverflow_tags.tag.name" as "tag_name",
      $"stackoverflow_tags.tag.frameworks.id" as "frameworks_id",
      $"stackoverflow_tags.tag.frameworks.name" as "frameworks_name"
    ).show()

    val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
    val df2 = spark.createDataFrame(donuts).toDF("DonutName", "Price")

    df2.show()

    val priceColumnExists = df2.columns.contains("Price")
    println(s"Does price column exist = $priceColumnExists")

// How to get first row and values:
    val firstRow = df2.first()
    println(s"First row = $firstRow")

    val firstRowColumn1 = df2.first().get(0)
        println(s"First row column 1 = $firstRowColumn1")

    val firstRowColumnPrice = df2.first().getAs[Double]("Price")

    val arrayData = Seq(Row("James",List("Java","Scala"),Map("hair"->"black","eye"->"brown")),Row("Michael",List("Spark","Java",null),Map("hair"->"brown","eye"->null)),Row("Robert",List("CSharp",""),Map("hair"->"red","eye"->"")),Row("Washington",null,null),Row("Jefferson",List(),Map()))
    import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, ArrayType, MapType}
    val arraySchema = new StructType().add("name",StringType).add("knownLanguages", ArrayType(StringType)).add("properties", MapType(StringType,StringType))

    val df3 = spark.createDataFrame(spark.sparkContext.parallelize(arrayData),arraySchema)
    df3.printSchema()
    df3.show(false)

    df3.select($"name",explode($"knownLanguages")).show(false)

    df3.select($"name",explode($"properties")).show(false)

/*    val onlyNewData = todaySchemaRDD.subtract(yesterdaySchemaRDD)
    dataFrame1.except(dataFrame2)
    df1.exceptAll(df2)*/

   /* val datatoBeInserted = df3.select("id", "is_enabled", "code", "description", "gamme", "import_local", "marque", "type_marketing", "reference", "struct", "type_tarif", "family_id", "range_id", "article_type_id")
    val cleanedData = datatoBeInserted.dropDuplicates("id")
    val droppedData = datatoBeInserted.except(cleanedData)*/

    val data = List(("James","","Smith","36636","M",60000),
      ("Michael","Rose","","40288","M",70000),
      ("Robert","","Williams","42114","",400000),
      ("Maria","Anne","Jones","39192","F",500000),
      ("Jen","Mary","Brown","","F",0))

    val cols = Seq("first_name","middle_name","last_name","dob","gender","salary")
    val df8 = spark.createDataFrame(data).toDF(cols:_*)

    val df9 = df8.withColumn("new_gender", when(col("gender") === "M","Male").when(col("gender") === "F","Female").otherwise("Unknown"))


    val dataDF = Seq(
      (66, "a", "4"), (67, "a", "0"), (70, "b", "4"), (71, "d", "4"
      )).toDF("id", "code", "amt")
    dataDF.withColumn("new_column",
      when(col("code") === "a" || col("code") === "d", "A")
        .when(col("code") === "b" && col("amt") === "4", "B")
        .otherwise("A1"))
      .show()



  }

}
