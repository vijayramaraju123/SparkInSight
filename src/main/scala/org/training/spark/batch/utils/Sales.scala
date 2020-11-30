package org.training.spark.batch.utils


case class Sales(transactionId:Int,customerId:Int,itemId:Int,amountPaid:Double)

case class SalesRecord(transactionId:String,customerId:Int,itemId:Int,amount:Double)

case class Customers(customerId:Int,name:String)