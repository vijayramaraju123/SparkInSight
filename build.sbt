name := "SparkInSight"

version := "0.1"

scalaVersion := "2.11.8"



libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.1"

libraryDependencies += "com.databricks" %% "spark-xml" % "0.4.1"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.47"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.1"

libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.6.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.1"
