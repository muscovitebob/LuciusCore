package LuciusBack

import org.scalatest.FlatSpec
import L1000._

class L1000Tests extends FlatSpec {

  info("Testing valueVector2RankVector")

  "valueVector2RankVector" should "convert a simple value vector to the rank vector " in {
    val vector:ValueVector = Array(1.,2.,3.,4.,5.)
    assert(valueVector2RankVector(vector) === Array(0.,1.,2.,3.,4.))
  }

  info("Testing valueVector2AvgRankVector")

  "valueVector2AvgRankVector" should "convert a value vector to the rank vector " in {
    val v = Array(-1.,2.,2.,-4.,5.)
    assert(valueVector2AvgRankVector(v) === Array(0.,1.5,1.5,-3.,4.))
  }

  it should "convert an empty value vector to the empty rank vector " in {
    val v:ValueVector = Array()
    assert(valueVector2AvgRankVector(v) === Array())
  }

  it should "convert an all-equal value vector to the correct rank vector " in {
    // ranks would be 4,3,2,1,0 but then use average, which is 4+3+2+1+0/4 = 10/5 = 2
    val vector:ValueVector = Array(1.,1.,1.,1.,1.)
    assert(valueVector2AvgRankVector(vector) === Array(2.,2.,2.,2.,2.))
  }

  info("Testing valueVector2AvgRankVectorWithZeros")

  "valueVector2AvgRankVectorWithZeros" should "convert a value vector to the rank vector " in {
    val v = Array(-1.,2.,2.,-4.,5.)
    assert(valueVector2AvgRankVector(v) === Array(0.,1.5,1.5,-3.,4.))
  }

  it should "convert an empty value vector to the empty rank vector " in {
    val v:ValueVector = Array()
    assert(valueVector2AvgRankVectorWithZeros(v) === Array())
  }

  it should "convert an all-equal value vector to the correct rank vector " in {
    // ranks would be 5,4,3,2,1 but then use average, which is 5+4+3+2+1/5 = 15/5 = 3
    val vector:ValueVector = Array(1.,1.,1.,1.,1.)
    assert(valueVector2AvgRankVectorWithZeros(vector) === Array(3.,3.,3.,3.,3.))
  }

  it should "avoid counting zero expressions for the ranks " in {
    val vector:ValueVector = Array(-6.,3.,4.,0.,0.)
    assert(valueVector2AvgRankVectorWithZeros(vector) === Array(-3.,1.,2.,0.,0.))
  }


  info("Testing orderedSignature2RankVector")

  val genes = Array("g1", "g2", "g3", "g4")

  "orderedSignature2RankVector" should "convert a simple index signature to the rank vector" in {
    val s:OrderedSignature = Array("-g2")
    assert(orderedSignature2RankVector(s, genes) === Array(0.,-1,0., 0.))
  }

  it should "convert a more complicated index signature to the rank vector" in {
    val s:OrderedSignature = Array("g3","-g1","-g4")
    assert(orderedSignature2RankVector(s, genes) === Array(-2.0, 0., 3.0, -1.0))
  }

  info("Testing rankVector2OrderedSignature")

  "rankVector2OrderedSignature" should "convert a simple rank vector to a simple signature" in {
    val v:RankVector= Array(0.,-1,0., 0.)
    assert(rankVector2OrderedSignature(v, genes) === Array("-g2"))
  }

  it should "not take into account zero ranks" in {
    val v:RankVector = Array(-2.0, 0., 3.0, -1.0)
    assert(rankVector2OrderedSignature(v, genes) === Array("g3","-g1","-g4"))
  }

  it should "convert a more complicated rank vector" in {
    val v:RankVector = Array(-2.0, 4.0, 3.0, -1.0)
    assert(rankVector2OrderedSignature(v, genes) === Array("g2", "g3", "-g1","-g4"))
  }

  it should "return the identity when composed with orderedSignature2RankVector" in {
  val v:RankVector = Array(-2.0, 4.0, 3.0, -1.0)
  assert(orderedSignature2RankVector(rankVector2OrderedSignature(v, genes), genes) === v)
  }



  info("Testing Connection score calculation")

  "Connection score" should "give correct result" in {
    val x: RankVector = Array(3., 2., 1., 0.)
    val y: RankVector = Array(0., 1., 2., 3.)
    assert(connectionScore(x,y) === (3.*0. + 2.*1. + 1.*2. + 0.*3.)/14)
  }

  it should "give zero when one vector contains only 1 gene" in {
    val x: RankVector = Array(3., 2., 1., 0.)
    val y: RankVector = Array(0., 0., 0., 1.)
    assert(connectionScore(x,y) === 0.)
  }

  it should "give 1. for equal vectors" in {
    val x: RankVector = Array(3., 2., 1., 0.)
    val y: RankVector = Array(3., 2., 1., 0.)
    assert(connectionScore(x,y) === 1.)
  }


  info("Testing median")

  "median" should "take the middle element when odd number of elements" in {
    val v:ValueVector = Array(-5.,3.,-1.,0.,2.)
    assert(median(v) === 0.)
  }

  it should "take the average of the middle elements when even number of elements" in {
    val v:ValueVector = Array(-5.,5.,-1.,1.)
    assert(median(v) === 0.)
  }


  info("signature creation from DB")

  /** We're running into an edge case with these tests:
    * When you want a total length of less than 3 for the signature, it clashes with the
    * number of significant genes in the signature.
    * This is solved by not looking at the result of the cutoff/filter function,
    * but rather to count the number of significant entries explicitely
    */

  "generateSignature" should "give correct result for all significant case" in {
    val genes = Array("a", "b", "c")
    val v:Array[(Double,Double)] =
      Array(  (-9.  , .1),        // a
              ( 4.  , .11),       // b
              ( -1. , .001)       // c
      )

    assert(generateSignature(v, genes, 0.2, 3) === Array("-a", "b", "-c"))
  }

  it should "give correct when length is limited" in {
    val genes = Array("a", "b", "c")
    val v:Array[(Double,Double)] =
      Array(  (-9.  , .1),        // a
              ( 4.  , .11),       // b
              ( -1. , .001)       // c
      )

    assert(generateSignature(v, genes, 0.2, 1) === Array("-a"))
  }

  it should "give correct result for only 1 significant gene with negative sign" in {
    val genes = Array("a", "b", "c")
    val v:Array[(Double,Double)] =
      Array(  (-9.  , .1),        // a
              ( 4.  , .11),       // b
              ( -1. , .001)       // c
      )

    assert(generateSignature(v, genes, 0.05, 3) === Array("-c","b"))
  }

  it should "give correct result for only 1 significant gene with positive sign" in {
    val genes = Array("a", "b", "c")
    val v:Array[(Double,Double)] =
      Array(  (-9.  , .1),        // a
              ( 4.  , .001),       // b
              ( -1. , .11)       // c
      )

    assert(generateSignature(v, genes, 0.05, 3) === Array("b","-a"))
  }


  it should "give correct result for no significant genes" in {
    val genes = Array("a", "b", "c")
    val v:Array[(Double,Double)] =
      Array(  (-9.  , .1),        // a
              ( 4.  , .11),       // b
              ( -1. , .001)       // c
      )

    assert(generateSignature(v, genes, 0.0001, 3) === Array("-c","b"))
  }


}