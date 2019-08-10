package com.dataintuitive.luciuscore.analytical

import com.dataintuitive.luciuscore.analytical.Bing.GeneType
import org.scalatest.FlatSpec
import com.dataintuitive.luciuscore.analytical.GeneAnnotations._
import com.dataintuitive.luciuscore.analytical.Signatures._


class SignaturesTest extends FlatSpec {

  val annotationsV2 = new GeneAnnotationsDb(Array(
    new GeneAnnotationRecord("200814_at", Some(GeneType.Landmark), None, None, Some("PSME1"), None, None),
    new GeneAnnotationRecord("222103_at", Some(GeneType.Landmark), None, None, Some("ATF1"), None, None),
    new GeneAnnotationRecord("201453_x_at", Some(GeneType.Landmark), None, None, Some("RHEB"), None, None),
    new GeneAnnotationRecord("200059_s_at", Some(GeneType.Landmark), None, None, Some("RHOA"), None, None),
    new GeneAnnotationRecord("220034_at", Some(GeneType.Landmark), None, None, Some("RHEB"), None, None)
  ))

  val symbols1 = SymbolSignatureV2(Array("RHEB"))
  val probesets1 = ProbesetidSignatureV2(Array("200059_s_at"))
  val indices1 = IndexSignatureV2(Array(2))

  "SymbolSignatureV2" should "correctly translate to probesets" in {
    val probeset2 = symbols1.translate2Probesetid(annotationsV2)
    assert(probeset2.signature.toList == List("201453_x_at", "220034_at"))
  }

  it should "correctly translate to indices" in {
    val indices2 = symbols1.translate2Index(annotationsV2)
    assert(indices2.signature.toList == List(3, 5))
  }


  /**
  "ProbesetidSignatureV2" should "correctly translate to symbols" in {
    val symbols2 = probesets1.translate2Symbol(annotationsV2)
    assert(symbols2.signature.toList == List("RHOA"))
  }

  "IndexSignatureV2" should "correctly translate to symbols" in {
    val symbols2 = indices1.translate2Symbol(annotationsV2)
    assert(symbols2.signature.toList == List("ATF1"))
  }**/

  it should "correctly translate to probesets again" in {
    val probesets2 = indices1.translate2Probeset(annotationsV2)
    assert(probesets2.signature.toList == List("222103_at"))
  }

  val symbols2 = SymbolSignatureV2(Array("-RHEB"))

  "SymboSignatureV2 with -ve" should "correctly preserve the sign converting to probesets" in {
    val probesets2 = symbols2.translate2Probesetid(annotationsV2)
    assert(probesets2.signature.toList == List("-201453_x_at", "-220034_at"))
  }

  it should "correctly preserve the sign converting to indices" in {
    val indices2 = symbols2.translate2Index(annotationsV2)
    assert(indices2.signature.sameElements(Array(-3, -5)))
  }

  val probesets2 = ProbesetidSignatureV2(Array("-201453_x_at", "-220034_at"))

  /**
  "ProbesetSignatureV2 -ve" should "correctly preserve to symbol" in {
    val symbols3 = probesets2.translate2Symbol(annotationsV2)
    assert(symbols3.signature.sameElements(Array("-RHEB")))
  }**/

  it should "correctly preserve to indices" in {
    val indices3 = probesets2.translate2Index(annotationsV2)
    assert(indices3.signature.toList == List(-3, -5))
  }

  val indices2 = IndexSignatureV2(Array(-2, -3))

  it should "correctly preserve to probeset" in {
    val probeset4 = indices2.translate2Probeset(annotationsV2)
    assert(probeset4.signature.toList == List("-222103_at", "-201453_x_at"))
  }

  /**

  "IndexSignatureV2 -ve" should "correctly preserve to symbol" in {
    val symbols4 = indices2.translate2Symbol(annotationsV2)
    assert(symbols4.signature.toList == List("-ATF1", "-RHEB"))
  }

  val indices3 = IndexSignatureV2(Array(-3, 5))
  "IndexSignatureV2" should "annihilate signs" in {
    val symbols5 = indices3.translate2Symbol(annotationsV2)
    assert(symbols5.signature.toList == List())
  }

  val probesets3 = ProbesetidSignatureV2(Array("-201453_x_at", "220034_at"))
  "ProbesetSignatureV2" should "annihilate signs" in {
    val symbols6 = probesets3.translate2Symbol(annotationsV2)
    assert(symbols6.signature.toList == List())
  }**/

  val symbols7 = SymbolSignatureV2(Array("RHEB", "-ATF1"))

  "SymbolSignatureV2" should "correctly create ordered ranks" in {
    assert(symbols7.r.toList == List(2, -1))
  }

  val symbols8 = SymbolSignatureV2(Array("RHEB", "-ATF1"), ordered = false)

  "SymbolSignatureV2" should "correctly create unordered ranks" in {
    assert(symbols8.r.toList == List(1, -1))
  }

}
