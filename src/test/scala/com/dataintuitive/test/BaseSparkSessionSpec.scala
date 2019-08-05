package com.dataintuitive.test

import org.apache.spark.sql.SparkSession

object BaseSparkSessionSpec {
  lazy val spark = SparkSession.builder.master("local[*]").appName("TestWithSQL").getOrCreate
}

trait BaseSparkSessionSpec {
  lazy val spark = BaseSparkSessionSpec.spark
}
