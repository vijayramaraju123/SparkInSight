package org.training.spark.ramAssignmnt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
/*


object DataFrameJoin {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    case class Person(name: String, age: Int, personid: Int)
    case class Profile(profileName: String, personid: Int, profileDescription: String)
    /**
      * * @param joinTypes Type of join to perform. Default `inner`. Must be one of:
      * *                 `inner`, `cross`, `outer`, `full`, `full_outer`, `left`, `left_outer`,
      * *                 `right`, `right_outer`, `left_semi`, `left_anti`.
      */
    val joinTypes = Seq(
      "inner"
      , "outer"
      , "full"
      , "full_outer"
      , "left"
      , "left_outer"
      , "right"
      , "right_outer"
      , "left_semi"
      , "left_anti" //, "cross"
    )
    val personDataFrame = spark.createDataFrame(
      Person("Nataraj", 45, 2)
        :: Person("Srinivas", 45, 5)
        :: Person("Ashik", 22, 9)
        :: Person("Deekshita", 22, 8)
        :: Person("Siddhika", 22, 4)
        :: Person("Madhu", 22, 3)
        :: Person("Meghna", 22, 2)
        :: Person("Snigdha", 22, 2)
        :: Person("Harshita", 22, 6)
        :: Person("Ravi", 42, 0)
        :: Person("Ram", 42, 9)
        :: Person("Chidananda Raju", 35, 9)
        :: Person("Sreekanth Doddy", 29, 9)
        :: Nil)
    val profileDataFrame = spark.createDataFrame(
      Profile("Spark", 2, "SparkSQLMaster")
        :: Profile("Spark", 5, "SparkGuru")
        :: Profile("Spark", 9, "DevHunter")
        :: Profile("Spark", 3, "Evangelist")
        :: Profile("Spark", 0, "Committer")
        :: Profile("Spark", 1, "All Rounder")
        :: Nil
    )
    println("Case 1:  First example inner join  ")
    val df_asPerson = personDataFrame.as("dfperson")
    val df_asProfile = profileDataFrame.as("dfprofile")
    val joined_df = df_asPerson.join(
      df_asProfile
      , col("dfperson.personid") === col("dfprofile.personid")
      , "inner")



    // you can do alias to refer column name with aliases to  increase readability
    joined_df.select(
      col("dfperson.name")
      , col("dfperson.age")
      , col("dfprofile.profileName")
      , col("dfprofile.profileDescription"))
      .show

    println("Case 2:  All Spark supported joins in a loop example inner join  ")

    joinTypes.zipWithIndex.foreach {
      case (element, index) => println(s" Index :$index -> ${element.toUpperCase()}")
    }
    println("all joins in a loop")
    joinTypes.zipWithIndex.foreach { case (joinType, index) =>
      println(s"-----> $index ${joinType.toUpperCase()} JOIN")
      df_asPerson.join(right = df_asProfile, Seq("personid"), joinType)
        .orderBy("personid")
        .show()
    }
    println(
      """ Case 3  Cartesian product:
        |Till 1.x  cross join is :  df_asPerson.join(df_asProfile)
        |
        | Explicit Cross Join in 2.x :
        |
        | Cartesian joins are very expensive without an extra filter that can be pushed down.
        |
        | cross join or cartesian product
        |
        |
    """.stripMargin)

    val crossJoinDf = df_asPerson.crossJoin(right = df_asProfile)
    crossJoinDf.show(200, false)
    crossJoinDf.explain()
    println("number of rows of cartesian product " + crossJoinDf.count.toString)

    println("Case 4 :   createOrReplaceTempView example ")
    println(
      """
        |Creates a local temporary view using the given name. The lifetime of this
        |   temporary view is tied to the [[SparkSession]] that was used to create this Dataset.
      """.stripMargin)

    df_asPerson.createOrReplaceTempView("dfperson");
    df_asProfile.createOrReplaceTempView("dfprofile")
    val sql =
      s"""
         |SELECT dfperson.name
         |, dfperson.age
         |, dfprofile.profileDescription
         |  FROM  dfperson JOIN  dfprofile
         | ON dfperson.personid == dfprofile.personid
    """.stripMargin
    println(s"createOrReplaceTempView  sql $sql")
    val sqldf = spark.sql(sql)
    sqldf.show
    println(
      """
        |Case 5 :
        |**** EXCEPT DEMO ***
        |
  """.stripMargin)
    println("Case 5.1 df_asPerson.except(df_asProfile) Except demo")
    df_asPerson.except(df_asProfile).show


    println("Case 5.2 df_asProfile.except(df_asPerson) Except demo")
    df_asProfile.except(df_asPerson).show()


  }

}

*/
