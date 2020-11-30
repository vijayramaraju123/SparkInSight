package org.training.spark.xother

import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object ProgrammaticSchema {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("programmaticschema")
    val sc: SparkContext = new SparkContext(conf)
    //sc.setLogLevel("INFO")
    val sqlContext = new SQLContext(sc)

    val salesRDD = sc.textFile(args(1))

    val schema =
      StructType(
        Array(StructField("transactionId", IntegerType, true),
          StructField("customerId", IntegerType, true),
          StructField("itemId", IntegerType, true),
          StructField("amountPaid", DoubleType, true))
      )

    val rowRDD = salesRDD.filter(line => !line.startsWith("transactionId"))
      .map(_.split("\\|"))
      .map(p => Row(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble))

    val schemaDF = sqlContext.createDataFrame(rowRDD,schema)

    schemaDF.printSchema()
    schemaDF.show()

        val testDF = sqlContext.read.format("csv")
          //.option("inferSchema", "false")
          .option("delimiter", "|")
          .option("mode","DROPMALFORMED")
          .schema(schema)
          .load(args(1))

//    testDF.printSchema()
//    testDF.select("transactionId","amountPaid").show

//    val dfSchema = testDF.schema
//
//    val columns = testDF.columns.filter(x => x.contains("Id"))
//
//    testDF.select(columns.head,columns.tail:_*).show()
//
//    println(dfSchema)
//    println(columns.toList)

  }

}
