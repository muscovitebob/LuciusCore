package com.dataintuitive.luciuscore.io

import com.dataintuitive.luciuscore.io.BingIO._
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import com.dataintuitive.test.BaseSparkSessionSpec

class BingIOTest extends FlatSpec with BaseSparkSessionSpec with Matchers {

  val bingFilepath = "src/main/resources/BING.csv"

  "Reading BING.csv into an RDD of case classes" should "work" in {
    val parsedBing = readBING(spark, bingFilepath)
    assert(parsedBing.take(1).head.isInstanceOf[BINGrow])
  }

}
