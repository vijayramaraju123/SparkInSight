import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


case class Sales(tid:Int,custId:Int,itemId:Int,amount:Double)

object testUdaf extends App{
  val conf = new SparkConf().setAppName("cache example")
  conf.setMaster("local")
  val sc = new SparkContext(conf)
  val sql = new SQLContext(sc)

  val salesRDD = sc.textFile(args(0))

  val caseSales = salesRDD.filter(x => !x.startsWith("tran")).map(x => {
    val arr = x.split(",")
    Sales(arr(0).toInt,arr(1).toInt,arr(2).toInt,arr(3).toDouble)
  })

  val salesDF = sql.createDataFrame(caseSales)

  salesDF.printSchema()

  //salesDF.groupBy("custId").agg(avg("amount").alias("avg")).show()

//  sql.udf.register("customAvg", new CustomAvg)
//  val customAvg = new CustomAvg
  val cnt = new CustomCount

  salesDF.groupBy("custId")
    .agg(avg("amount").alias("avg"),count("tid"),cnt(col("tid")))
    .show()

  val ids = sql.range(1, 20)
  ids.registerTempTable("ids")
  val df = sql.sql("select id, id % 3 as group_id from ids")
  //df.registerTempTable("simple")

//  df.show()
//
//  df.groupBy("group_id").agg(gm(col("id")).as("GeometricMean")).show()

}


class CustomCount extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(List(StructField("value", IntegerType)))

  // This is the internal fields you keep for computing your aggregate.

  def bufferSchema = StructType(Array(
    StructField("cnt", LongType)
  ))

  // This is the output type of your aggregatation function.
  override def dataType: DataType = LongType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  //  override def initialize(buffer: MutableAggregationBuffer): Unit = {
  //    buffer(0) = 0L
  //    buffer(1) = 1.0
  //  }

  def initialize(buffer: MutableAggregationBuffer) = {
    buffer(0) = 0L // set sum to zero
  }

  // This is how to update your buffer schema given an input.

  def update(buffer: MutableAggregationBuffer, input: Row) = {
    buffer(0) = buffer.getLong(0) + 1
  }

  // This is how to merge two objects with the bufferSchema type.

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  def evaluate(buffer: Row) = {
    buffer.getLong(0)//.toDouble
  }
}