package com.dataintuitive.test

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{Matchers, Spec}

/**
  * @author Thomas Moerman
  * Shamelesly copied from
  * https://github.com/tmoerman/vcf-comp/blob/master/src/test/scala/org/tmoerman/test/spark/BaseSparkContextSpec.scala
  */
object BaseSparkContextSpec {

  lazy val conf = new SparkConf()
    .setAppName("Test")
    .setMaster("local[*]")
//    .set("spark.kryo.registrator", "org.tmoerman.adam.fx.serialization.AdamFxKryoRegistrator")
//    .set("spark.kryo.referenceTracking", "true")
//    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  lazy val sc = new SparkContext(conf)

}

trait BaseSparkContextSpec {

  lazy val sc = BaseSparkContextSpec.sc

}
