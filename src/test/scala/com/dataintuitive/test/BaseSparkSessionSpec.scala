package com.dataintuitive.test

import org.apache.spark.sql.SparkSession

object BaseSparkSessionSpec {
  lazy val spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate
  spark.sparkContext.setLogLevel("ERROR") // make spark stop spewing too much info when running sbt test
}

trait BaseSparkSessionSpec {
  lazy val spark = BaseSparkSessionSpec.spark
}
