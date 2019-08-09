package com.dataintuitive.luciuscore.analytical

import com.dataintuitive.luciuscore.analytical.Bing._
import com.dataintuitive.luciuscore.analytical.io.BingIO._
import com.dataintuitive.test.BaseSparkSessionSpec
import org.scalatest.FlatSpec

class BingTest extends FlatSpec with BaseSparkSessionSpec{
  val bingFilepath = "src/main/resources/BING.csv"
  val parsedBing = readBING(spark, bingFilepath)
  val bingInfo1 = new bingInformation(parsedBing)

  "bingInformation" should "instantiate correctly" in {
    assert(bingInfo1.bingStatus.isInstanceOf[Map[String, GeneType.GeneType]])
    assert(bingInfo1.symbolToRow.isInstanceOf[Map[String, BINGrow]])
  }



}
