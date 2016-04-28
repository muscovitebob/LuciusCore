package com.dataintuitive.luciuscore.lowlevel

import com.dataintuitive.luciuscore._
import TransformationFunctions._
import org.scalatest.FlatSpec

/**
  * Created by toni on 27/04/16.
  */
class TransformationFunctionsTest extends FlatSpec {

  info("Test transformation functions")

  info("Test median")

  "median" should "take the middle element when odd number of elements" in {
    val v:ValueVector = Array(-5.0,3.0,-1.0,0.0,2.0)
    assert(median(v) === 0.0)
  }

  it should "take the average of the middle elements when even number of elements" in {
    val v:ValueVector = Array(-5.0,5.0,-1.0,1.0)
    assert(median(v) === 0.0)
  }

  info("Test valueVectorSelection2ValueVector")

  "valueVectorSelection2ValueVector" should "derive the median values for t-stats for a set of vectors" in {
    val t1 = Array(-1.0, 1.0, -1.0, 0.0)
    val t2 = Array(-4.0, 2.0, -1.0, 0.0)
    val t3 = Array(-5.0, 3.0, -1.0, 0.0)

    val p = Array(0.0, 0.0, 0.0, 0.0)

    val selection =
      Array(
        ("pwid1", t1, p),
        ("pwid2", t2, p),
        ("pwid3", t3, p))

    assert(valueVectorSelection2ValueVector(selection) === Array(-4.0, 2.0, -1.0, 0.0))
  }

  info("Test valueVector2RankVector")

  "valueVector2RankVector" should "convert a simple value vector to the rank vector " in {
    val tp = ("pwid1", Array(1.0,2.0,3.0,4.0,5.0), Array(0.0,0.0,0.0,0.0,0.0))
    assert(valueVector2RankVector(tp) === Array(0.0,1.0,2.0,3.0,4.0))
  }

  info("Test valueVector2AvgRankVector")

  "valueVector2AvgRankVector" should "convert a value vector to the rank vector " in {
    val v = Array(-1.0,2.0,2.0,-4.0,5.0)
    val tp = ("pwid1", v, Array(0.0,0.0,0.0,0.0,0.0))
    assert(valueVector2AvgRankVector(tp) === Array(0.0,1.5,1.5,-3.0,4.0))
  }

  it should "convert an empty value vector to the empty rank vector " in {
    val v:ValueVector = Array()
    val empty:ValueVector = Array()
    val tp = ("pwid1", v, empty)
    assert(valueVector2AvgRankVector(tp) === Array())
  }

  it should "convert an all-equal value vector to the correct rank vector " in {
    // ranks would be 4,3,2,1,0 but then use average, which is 4+3+2+1+0/4 = 10/5 = 2
    val v:ValueVector = Array(1.0,1.0,1.0,1.0,1.0)
    val tp = ("pwid1", v, Array(0.0,0.0,0.0,0.0,0.0))
    assert(valueVector2AvgRankVector(tp) === Array(2.0,2.0,2.0,2.0,2.0))
  }

  info("Test valueVector2AvgRankVectorWithZeros")

  "valueVector2AvgRankVectorWithZeros" should "convert a simple value vector to a rank vector " in {
    val v = Array(-1.0,-0.5,2.0,-4.0,5.0)
    val tp = ("pwid1", v, Array(0.0,0.0,0.0,0.0,0.0))
    assert(valueVector2AvgRankVectorWithZeros(tp) === Array(-2.0,-1,3.0,-4.0,5.0))
  }

  it should "convert a value vector to the rank vector with duplicates" in {
    val v = Array(-1.0,2.0,2.0,-4.0,5.0)
    val tp = ("pwid1", v, Array(0.0,0.0,0.0,0.0,0.0))
    assert(valueVector2AvgRankVectorWithZeros(tp) === Array(-1.0,2.5,2.5,-4.0,5.0))
  }

  it should "convert an empty value vector to the empty rank vector " in {
    val v:ValueVector = Array()
    val tp = ("pwid1", v, Array(0.0,0.0,0.0,0.0,0.0))
    assert(valueVector2AvgRankVectorWithZeros(tp) === Array())
  }

  it should "convert an all-equal value vector to the correct rank vector " in {
    // ranks would be 5,4,3,2,1 but then use average, which is 5+4+3+2+1/5 = 15/5 = 3
    val v:ValueVector = Array(1.0,1.0,1.0,1.0,1.0)
    val tp = ("pwid1", v, Array(0.0,0.0,0.0,0.0,0.0))
    assert(valueVector2AvgRankVectorWithZeros(tp) === Array(3.0,3.0,3.0,3.0,3.0))
  }

  it should "avoid counting zero expressions for the ranks " in {
    val v:ValueVector = Array(-6.0,3.0,4.0,0.0,0.0)
    val tp = ("pwid1", v, Array(0.0,0.0,0.0,0.0,0.0))
    assert(valueVector2AvgRankVectorWithZeros(tp) === Array(-3.0,1.0,2.0,0.0,0.0))
  }

}
