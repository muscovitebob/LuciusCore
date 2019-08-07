package com.dataintuitive.luciuscore

import com.dataintuitive.luciuscore.Bing.GeneType
import com.dataintuitive.luciuscore.GeneModel.{GeneAnnotationV2, GenesV2}
import com.dataintuitive.luciuscore.Model._
import com.dataintuitive.luciuscore.SignatureModel._
import org.scalatest.{FlatSpec, Matchers}

class SignatureModelTest extends FlatSpec with Matchers {

  info("Test companion object")

  val validSymbolSignatureCreated = Signature(Array("symbol2", "-symbol1"), "symbol")
  val validProbesetidSignatureCreated = Signature(Array("psid2", "-psid1"), "probesetid")

  "Companion object for Symbol" should "create correct object" in {
    validSymbolSignatureCreated.toString should equal ("Signature of type symbol: [symbol2,-symbol1]")
  }

  "Companion object for Probeset" should "create correct object" in {
    validProbesetidSignatureCreated.toString should equal ("Signature of type probesetid: [psid2,-psid1]")
  }


  info("Test GeneSignature model, first just symbol and probesetid")

  // Translation is done using a dictionary
  val dict:GeneDictionary = Map("symbol1" -> "psid1", "symbol2" -> "psid2")

  val validSymbolSignature = SymbolSignature(Array("symbol2", "-symbol1"))
  val validProbesetidSignature = ProbesetidSignature(Array("psid2", "-psid1"))
  val invalidSymbolSignature = SymbolSignature(Array("-symbol1", "symbol3"))
  val invalidProbesetidSignature = ProbesetidSignature(Array("-psid1", "psid3"))

  "The notation for a symbol signature" should "be correct" in {
    assert(validSymbolSignature.notation === "symbol")
    assert(validProbesetidSignature.notation === "probesetid")
  }

  "A valid SymbolSignature" should "translate into a valid ProbesetidSignature" in {
    assert(validSymbolSignature.translate2Probesetid(dict).signature === validProbesetidSignature.signature)
  }

  "A valid ProbesetidSignature" should "translate into a valid SymbolSignature" in {
    assert(validProbesetidSignature.translate2Symbol(dict).signature === validSymbolSignature.signature)
  }

  "Two consecutive translations" should "return identity" in {
    assert(validProbesetidSignature.translate2Symbol(dict).translate2Probesetid(dict).signature === validProbesetidSignature.signature)
    assert(validSymbolSignature.translate2Probesetid(dict).translate2Symbol(dict).signature === validSymbolSignature.signature)
  }

  "Safe translation" should "return options instead of Strings" in {
    assert(invalidProbesetidSignature.safeTranslate2Symbol(dict) === Array(Option("-symbol1"), None))
    assert(invalidSymbolSignature.safeTranslate2Probesetid(dict) === Array(Option("-psid1"), None))
  }

  "Translation" should "return OOPS for unknown entries" in {
    assert(invalidProbesetidSignature.translate2Symbol(dict).signature === Array("-symbol1", "OOPS"))
    assert(invalidSymbolSignature.translate2Probesetid(dict).signature === Array("-psid1", "OOPS"))
  }

  info("Test GeneSignature model, throw index-based signature in the mix")

  // Translation is done using a dictionary
  val indexDict = Map(1 -> "psid1", 2 -> "psid2")

  val indexSignature = new IndexSignature(Array("2", "-1"))

  "The notation for a index signature" should "be correct" in {
    assert(indexSignature.notation === "index")
  }

  "A probesetidSignature" should "translate into an index signature" in {
    assert(validProbesetidSignature.translate2Index(indexDict).toString === indexSignature.toString)
  }

  "A symbol signature" should "first be translated, then converted into an index signature" in {
    assert(validSymbolSignature.translate2Probesetid(dict).translate2Index(indexDict).toString === indexSignature.toString)
  }

  "An index signature" should "translate into the correct probesetid signature and symbol signature" in {
    assert(indexSignature.translate2Probesetid(indexDict).toString === validProbesetidSignature.toString)
    assert(indexSignature.translate2Probesetid(indexDict).translate2Symbol(dict).toString === validSymbolSignature.toString)
  }

  "An index signature" should "convert to signed int sparse signature" in {
    assert(indexSignature.asSignedInt === Array(2,-1))
  }

  info("Test the companion object")

  "The companion object" should "create a signature object of type symbol" in {
    assert(Signature(Array("symbol2", "-symbol1")).signature === validSymbolSignature.signature)
    assert(Signature(Array("symbol2", "-symbol1")).notation === "symbol")
  }

  it should "create a signature of provided type" in {
    assert(Signature(Array("Symbol"), notation = "symbol").notation === "symbol")
    assert(Signature(Array("Probesetid"), notation = "probesetid").notation === "probesetid")
    assert(Signature(Array("1"), notation = "index").notation === "index")
    // Default for undefined notation is symbol
    assert(Signature(Array("NA"), notation = "undefined").notation === "symbol")
  }



  info("test SignatureV2")

  val annotationsV2 = new GenesV2(Array(
    new GeneAnnotationV2("200814_at", GeneType.Landmark, None, None, Some("PSME1"), None, None),
    new GeneAnnotationV2("222103_at", GeneType.Landmark, None, None, Some("ATF1"), None, None),
    new GeneAnnotationV2("201453_x_at", GeneType.Landmark, None, None, Some("RHEB"), None, None),
    new GeneAnnotationV2("200059_s_at", GeneType.Landmark, None, None, Some("RHOA"), None, None),
    new GeneAnnotationV2("220034_at", GeneType.Landmark, None, None, Some("RHEB"), None, None)
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

  "ProbesetidSignatureV2" should "correctly translate to symbols" in {
    val symbols2 = probesets1.translate2Symbol(annotationsV2)
    assert(symbols2.signature.toList == List("RHOA"))
  }

  it should "correctly translate to indices" in {
    val indices2 = probesets1.translate2Index(annotationsV2)
    assert(indices2.signature.toList == List(4))
  }

  "IndexSignatureV2" should "correctly translate to symbols" in {
    val symbols2 = indices1.translate2Symbol(annotationsV2)
    assert(symbols2.signature.toList == List("ATF1"))
  }

  it should "correctly translate to probesets" in {
    val probesets2 = indices1.translate2Probeset(annotationsV2)
    assert(probesets2.signature.toList == List("222103_at"))
  }

  val symbols2 = SymbolSignatureV2(Array("-RHEB"))

  "SymboSignatureV2 with -ve" should "correctly preserve the sign converting to probesets" in {
    val probesets2 = symbols2.translate2Probesetid(annotationsV2)
    assert(probesets2.signature.toList == List("-201453_x_at", "-220034_at"))
  }

}
