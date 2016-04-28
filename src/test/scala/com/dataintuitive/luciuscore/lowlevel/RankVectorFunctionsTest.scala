package com.dataintuitive.luciuscore.lowlevel

import com.dataintuitive.luciuscore.IndexSignature
import com.dataintuitive.luciuscore.lowlevel.RankVectorFunctions._
import org.scalatest.FlatSpec

/**
  * Created by toni on 26/04/16.
  */
class RankVectorFunctionsTest extends FlatSpec {

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
