package com.dataintuitive.luciuscore.io

import org.scalatest.{FlatSpec, FunSuite}
import com.dataintuitive.luciuscore.io.StatsIOV2._
import com.dataintuitive.test.BaseSparkSessionSpec._
import org.apache.spark.rdd.RDD

class StatsIOV2Test extends FlatSpec {

  val tStats = loadFile(spark.sparkContext, "src/test/resources/tStats.txt", " ")

  "loadFile" should "return something resembling decency" in {
    assert(tStats.isInstanceOf[StatsData])
  }

  val dataMat = spark.sparkContext.parallelize(Array(Vector(0.25, 0.6), Vector(0.4, -0.1)))
    .zipWithIndex().map(_.swap)
  val tinyData = StatsData(Vector("sample1", "sample2"), Vector("probeset1", "probeset2"), dataMat)

  "transpose" should "work correctly" in {
    val transposed = spark.sparkContext.parallelize(Array(Vector(0.25, 0.4), Vector(0.6
      , -0.1)))
      .zipWithIndex().map(_.swap)

    assert(tinyData.transpose.dataMatrix.collect.toList == transposed.collect.toList)
  }


}
