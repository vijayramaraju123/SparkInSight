package advanced

import org.apache.spark.sql.SparkSession

object SparkMainTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName(getClass.getName)
      .getOrCreate();

    spark.sparkContext.setLogLevel("INFO")

    println("First SparkContext:")
    println("APP Name :" + spark.sparkContext.appName)
    println("Deploy Mode :" + spark.sparkContext.deployMode)
    println("Master :" + spark.sparkContext.master)



  }

}
