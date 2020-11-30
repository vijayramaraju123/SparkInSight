package org.training.spark.batch.utils

object FunctionUtils {
  def getDiscount(amount:Double):Double = {
    if(amount<2000) amount*0.1
    else amount*0.2
  }
}
