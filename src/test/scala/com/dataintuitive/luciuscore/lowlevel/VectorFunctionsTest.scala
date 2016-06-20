package com.dataintuitive.luciuscore.lowlevel

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FlatSpec
import VectorFunctions._
import com.dataintuitive.test.BaseSparkContextSpec

/**
  * Created by toni on 27/04/16.
  */
class VectorFunctionsTest extends FlatSpec with BaseSparkContextSpec {

  info("Test function on Vectors")

  "Simple transpose of transpose" should "return original dataset" in {
      val a:RDD[Array[String]] = sc.parallelize(
        Array(
          Array("1","2","3"),
          Array("4","5","6"),
          Array("7","8","9"))
      )
      val att:RDD[Array[String]] = transpose(transpose(a))
      assert( att.collect === a.collect )
    }

  "Transpose of non-rectangular matrix" should "return original dataset" in {
    val a:RDD[Array[String]] = sc.parallelize(
      Array(
        Array("1","2","3"),
        Array("4","5","6"))
    )
    val att:RDD[Array[String]] = transpose(transpose(a))
    assert( att.collect === a.collect )
  }

}
