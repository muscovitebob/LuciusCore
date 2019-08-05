package com.dataintuitive.test

import com.dataintuitive.test.BaseSparkSessionSpec._

trait BaseSparkContextSpec {

  lazy val sc = spark.sparkContext

}
