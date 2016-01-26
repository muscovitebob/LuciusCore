package LuciusBack

import org.scalatest.FlatSpec
import L1000._
import GeneFunctions._

class GeneFunctionsTest extends FlatSpec {

  info("Testing GeneFunctions: sign and abs")

  val tt:TranslationTable = Map("xg1" -> "yg1", "xg2" -> "yg2", "xg3" -> "yg1")

  "signGene of -g" should "return -" in {
    val gene:Gene = "-g"
    assert(signGene(gene) === "-")
  }

  "signGene of g" should "return empty string" in {
    val gene:Gene = "g"
    assert(signGene(gene) === "")
  }

  info("Testing GeneFunctions: translations")

  "translateGenes" should "translate an easy signature" in {
    val s:OrderedSignature = Array("xg1", "xg2")
    assert(translateGenes(s, tt) === Array("yg1", "yg2"))
  }

  "translateGenes" should "translate unknown genes in OOPS" in {
    val s:OrderedSignature = Array("xg1", "xg5")
    assert(translateGenes(s, tt) === Array("yg1", "OOPS"))
  }


}
