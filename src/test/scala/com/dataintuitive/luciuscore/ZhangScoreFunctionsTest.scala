package com.dataintuitive.luciuscore

import com.dataintuitive.luciuscore.Model._
import com.dataintuitive.luciuscore.ZhangScoreFunctions._
import com.dataintuitive.luciuscore.io.SampleCompoundRelationsIO.loadSampleCompoundRelationsFromFileV2
import com.dataintuitive.test.BaseSparkContextSpec
import org.scalatest.FlatSpec

/**
  * Created by toni on 26/04/16.
  */
class ZhangScoreFunctionsTest extends FlatSpec with BaseSparkContextSpec {

  info("Testing Connection score calculation")

  "Connection score" should "give correct result" in {
    val x: RankVector = Array(3.0, 2.0, 1.0, 0.0)
    val y: RankVector = Array(0.0, 1.0, 2.0, 3.0)
    assert(connectionScore(x,y) === (3.0*0.0 + 2.0*1.0 + 1.0*2.0 + 0.0*3.0)/14.0)
  }

  it should "give zero when one vector contains only 1 gene" in {
    val x: RankVector = Array(3.0, 2.0, 1.0, 0.0)
    val y: RankVector = Array(0.0, 0.0, 0.0, 1.0)
    assert(connectionScore(x,y) === 0.0)
  }

  it should "give 1. for equal vectors" in {
    val x: RankVector = Array(3.0, 2.0, 1.0, 0.0)
    val y: RankVector = Array(3.0, 2.0, 1.0, 0.0)
    assert(connectionScore(x,y) === 1.0)
  }

  info("Testing rank vector scoring")

  // load relations data in order to get an example DbRow object
  val sampleCompoundRelationsV2Source = "src/test/resources/v2/sampleCompoundRelations.txt"
  val aDbRow = loadSampleCompoundRelationsFromFileV2(sc, sampleCompoundRelationsV2Source).first
  // the example data does not have the r vectors we are testing, so construct new DbRow
  val newDbRow = DbRow(aDbRow.pwid,
    SampleAnnotations(aDbRow.sampleAnnotations.sample,
      Some(Array(2.0, 2.0, 2.0, 2.0)),
      Some(Array(2.0, 2.0, 2.0, 2.0)),
      Some(Array(2.0, 2.0, 2.0, 2.0))),
    aDbRow.compoundAnnotations)

  "queryDbRow function" should "give a numerical value for a single query" in {
    val x: RankVector = Array.fill(4){scala.util.Random.nextInt(10).asInstanceOf[Double]}
    assert(queryDbRow(newDbRow, x).values.toSeq.head.head.get.isInstanceOf[Double] === true)
  }

  it should "give a list of size two for two queries" in {
    val x: RankVector = Array.fill(4){scala.util.Random.nextInt(10).asInstanceOf[Double]}
    val y: RankVector = Array.fill(4){scala.util.Random.nextInt(10).asInstanceOf[Double]}
    assert(queryDbRow(newDbRow, x, y).values.toSeq.head.size === 2)
  }

}
