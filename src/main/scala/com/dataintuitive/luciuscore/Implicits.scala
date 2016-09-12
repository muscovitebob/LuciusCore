package com.dataintuitive.luciuscore

import com.dataintuitive.luciuscore.utilities.{PimpedRDD, SignedString}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


/**
  * A simple object containing handy implicits when working with L1000 data
  */
object Implicits extends Serializable {

  implicit def pimpRDD[V:ClassTag](rdd:RDD[V]):PimpedRDD[V] = new PimpedRDD(rdd)

  implicit def signString(string: String) = new SignedString(string)

}
