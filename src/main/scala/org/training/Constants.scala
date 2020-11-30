package org.training

object Constants {
  /*CASSANDRA INFO*/
  val CASSANDRA_KEYSPACE: String = "demand"
  val CASSANDRA_HOST: String = ""
  val CASSANDRA_ALL_HOST: String = ""
  val CASSANDRA_USER: String = "cassandra"
  val CASSANDRA_PASSWORD: String = "cassandra"
  val CASSANDRA_TARGET: String = ""
  val CASSANDRA_SOURCE: String = ""
  val CLOUDEDH_KEYSPACE: String = "aa_cloud"

  val SPARK_CASSANDRA_CONNECTION_HOST: String = "spark.cassandra.connection.host"

  /*HDFS INFO*/
  val HDFS_HOST: String = ""
  val HDFS_PORT: String = "8020"
  val HDFS_LOCAL_FOLDER: String = "/demand/"
  val HDFS_PATH: String = "hdfs://" + HDFS_HOST + ":" + HDFS_PORT + HDFS_LOCAL_FOLDER

  /*MEMSQL INFO*/
  val MEMSQL_HOST_VAL: String = ""
  val MEMSQL_PORT_VAL: Int = 3306
  val MEMSQL_DATABASE_VAL: String = "demand"
  val MEMSQL_USERNAME_VAL: String = "root"
  val MEMSQL_PASSWORD_VAL: String = ""

  /*Orders And link*/
  val ORDERS = "GENERATE_ORDERS_AND_LINK"
  val GENERATE_ALL_REPORTS = "GENERATE_ALL_REPORTS"
  /*SLI Orders*/
  val GENERATE_SLI_ORDERS_IND = "GENERATE_SLI_ORDERS_IND"
  val GENERATE_SLI_ORDERS_HDA = "GENERATE_SLI_ORDERS_HDA"
  /*File Progress*/
  val ORDER_FOLLOWUP_REPORT = "ORDER_FOLLOWUP_REPORT"
  val SALES_ORDER_REPORT = "SALES_ORDER_REPORT"
  val DATA_QUALITY_GPMA_REPORT = "DATA_QUALITY_GPMA_REPORT"
  val DATA_QUALITY_PART_REPORT = "DATA_QUALITY_PART_REPORT"
  val DATA_QUALITY_PBOM_REPORT = "DATA_QUALITY_PBOM_REPORT"

  val ORDERS_PLI_UPDATE = "ORDERS_PLI_UPDATE"
  val ORDERS_PLI_PBOM = "ORDERS_PLI_PBOM"
  val SLI_ORDERS_USING_IND = "SLI_ORDERS_USING_IND"
  val SLI_ORDERS_USING_HDA = "SLI_ORDERS_USING_HDA"

  val SLI_ORDERS_OFU_IND = "SLI_ORDERS_OFU_IND"
  val SLI_ORDERS_OFU_HDA = "SLI_ORDERS_OFU_HDA"

  // Manual addition of check data
  val REDIS_HOST = " Test Data "
  val REDIS_PORT = " Test Data "
  val REDIS_PASSWORD = " Test Data "
  val REDIS_DB = " Test Data "

  val ALLUXIO_URL = "test Data"
  val ALLUXIO_HOST = " test Data"
  val ALLUXIO_PORT = " test Data"
  val ALLUXIO_FOLDER = " test Data"
  val NO_OF_PARTITIONS = 8
  val LINKING_STAGE = " test Data"
  val ITEM_ALIAS_FINAL = " test Data"


  

}