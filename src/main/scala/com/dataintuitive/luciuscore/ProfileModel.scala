package com.dataintuitive.luciuscore

import com.dataintuitive.luciuscore.Model._
import org.apache.spark.rdd.RDD

object ProfileModel {
  /**
    * Object to deal with RDD[DbRow] AKA databases of transcriptomic profiles, against which connectivity scoring is possible
    */

  class ProfileDatabase(database: RDD[DbRow]) {

  }
}
