package com.dataintuitive.luciuscore

import com.dataintuitive.luciuscore.Model._
import com.dataintuitive.luciuscore.TransformationFunctions._
import org.scalatest.FlatSpec

/**
  * Created by toni on 27/04/16.
  */
class TransformationFunctionsTest extends FlatSpec {

  info("Test aggregateStats")

  "aggregateStats" should "derive the median values for t-stats for a set of vectors" in {
    val t1 = Array(-1.0, 1.0, -1.0, 0.0)
    val t2 = Array(-4.0, 2.0, -1.0, 0.0)
    val t3 = Array(-5.0, 3.0, -1.0, 0.0)

    val p = Array(0.0, 0.0, 0.0, 0.0)

    val selection =
      Array(
        (t1, p),
        (t2, p),
        (t3, p))

    assert(aggregateStats(selection) === Array(-4.0, 2.0, -1.0, 0.0))
  }

  info("Test stats2RankVector")

  "stats2RankVector" should "convert a simple value vector to a rank vector " in {
    val v = Array(-1.0,-0.5,2.0,-4.0,5.0)
    val tp = (v, Array(0.0,0.0,0.0,0.0,0.0))
    assert(stats2RankVector(tp) === Array(-2.0,-1,3.0,-4.0,5.0))
  }

  it should "convert a value vector to the rank vector with duplicates" in {
    val v = Array(-1.0,2.0,2.0,-4.0,5.0)
    val tp = (v, Array(0.0,0.0,0.0,0.0,0.0))
    assert(stats2RankVector(tp) === Array(-1.0,2.5,2.5,-4.0,5.0))
  }

  it should "convert an empty value vector to the empty rank vector " in {
    val v:ValueVector = Array()
    val tp = (v, Array(0.0,0.0,0.0,0.0,0.0))
    assert(stats2RankVector(tp) === Array())
  }

  it should "convert an all-equal value vector to the correct rank vector " in {
    // ranks would be 5,4,3,2,1 but then use average, which is 5+4+3+2+1/5 = 15/5 = 3
    val v:ValueVector = Array(1.0,1.0,1.0,1.0,1.0)
    val tp = (v, Array(0.0,0.0,0.0,0.0,0.0))
    assert(stats2RankVector(tp) === Array(3.0,3.0,3.0,3.0,3.0))
  }

  it should "avoid counting zero expressions for the ranks " in {
    val v:ValueVector = Array(-6.0,3.0,4.0,0.0,0.0)
    val tp = (v, Array(0.0,0.0,0.0,0.0,0.0))
    assert(stats2RankVector(tp) === Array(-3.0,1.0,2.0,0.0,0.0))
  }


  info("Test the functions for dealing with vectors")

  val indexSignature = new IndexSignature(Array("1", "-3"))
  val rankVector = signature2OrderedRankVector(indexSignature, 3)

  "An IndexGeneSignature" should "convert to a dense vector of given length" in {
    assert(rankVector === Array(2.0, 0.0, -1.0))
  }

  it should "convert to a dense vector of length smaller than the signature" in {
    assert(signature2OrderedRankVector(indexSignature, 1) === Array(2.0))
  }

  "An IndexGeneSignature" should "convert to a dense vector of given length for unordered as well" in {
    assert(signature2UnorderedRankVector(indexSignature, 3) === Array(1.0, 0.0, -1.0))
  }

  "Nonzero elements" should "return an array of non-zero elements" in {
    assert(nonZeroElements(rankVector) === Array((1, 2.0), (3, -1.0)))
  }

  "A rankVector" should "convert to a sparse signature" in {
    assert(rankVector2IndexSignature(rankVector).signature === Array("1", "-3"))
  }


}
