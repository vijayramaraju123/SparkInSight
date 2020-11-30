package org.training.spark.ramAssignmnt

import org.apache.spark.sql.SparkSession
import org.training.spark.ramAssignmnt.DataFrameCrreation.getClass

object CovidDataApp {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .master("local[*]")
      .appName(getClass.getName)
      .getOrCreate()

    val df1=spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("C:\\Users\\Incredible\\Documents\\run\\ramAssignmnt\\full_data.csv")

    df1.show()









  }

}
