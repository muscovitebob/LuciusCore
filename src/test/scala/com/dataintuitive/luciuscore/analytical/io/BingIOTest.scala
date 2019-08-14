package com.dataintuitive.luciuscore.analytical.io

import com.dataintuitive.luciuscore.analytical.io.BingIO._
import com.dataintuitive.test.BaseSparkSessionSpec
import org.scalatest.{FlatSpec, Matchers}

class BingIOTest extends FlatSpec with BaseSparkSessionSpec with Matchers {

  val bingFilepath = "src/test/resources/BING.csv"

  "Reading BING.csv into an RDD of case classes" should "work" in {
    val parsedBing = readBING(spark, bingFilepath)
    assert(parsedBing.take(1).head.isInstanceOf[BINGrow])
  }

}
