package com.dataintuitive.test

import org.apache.spark.{SparkConf, SparkContext}

object BaseSparkContextSpec {

  lazy val conf = new SparkConf()
    .setAppName("Test")
    .setMaster("local[*]")
    .set("spark.driver.allowMultipleContexts", "true")
  lazy val sc = new SparkContext(conf)

}

trait BaseSparkContextSpec {

  lazy val sc = BaseSparkContextSpec.sc

}
