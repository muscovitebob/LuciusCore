package com.dataintuitive.luciuscore.analytical

import com.dataintuitive.luciuscore.analytical.GeneAnnotations._
import com.dataintuitive.luciuscore.analytical.Bing.GeneType
import org.scalatest.FlatSpec

class GeneAnnotationsTest extends FlatSpec {

  val geneV2: GeneAnnotationRecord = new GeneAnnotationRecord("probesetidString",
    GeneType.Landmark,
    "entrezidString",
    "ensemblidString",
    "symbolString",
    "nameString",
    "familyString")

  "methods on a gene of V2" should "return the method field" in {
    assert(geneV2.name.get === "nameString")
    assert(geneV2.symbol.get === "symbolString")
    assert(geneV2.ensemblid.get === "ensemblidString")
    assert(geneV2.entrezid.get === "entrezidString")
    assert(geneV2.probesetid === "probesetidString")
    assert(geneV2.geneFamily.get === "familyString")
    assert(geneV2.dataType.get === GeneType.Landmark)
  }


  val annotationsV2 = new GeneAnnotationsDb(Array(
    new GeneAnnotationRecord("200814_at", Some(GeneType.Landmark), None, None, Some("PSME1"), None, None),
    new GeneAnnotationRecord("222103_at", Some(GeneType.Landmark), None, None, Some("ATF1"), None, None),
    new GeneAnnotationRecord("201453_x_at", Some(GeneType.Landmark), None, None, Some("RHEB"), None, None),
    new GeneAnnotationRecord("200059_s_at", Some(GeneType.Landmark), None, None, Some("RHOA"), None, None),
    new GeneAnnotationRecord("220034_at", Some(GeneType.Landmark), None, None, Some("RHEB"), None, None)
  ))

  "GenesV2" should "instantiate as a database of gene annotations correctly" in {
    assert(annotationsV2.genes.filter(_.symbol.get.head == "PSME1").head.probesetid == "200814_at")
  }

  "GenesV2" should "instantiate the symbol -> probeset dict correctly" in {
    val correct = List("201453_x_at", "220034_at")
    assert(annotationsV2.symbol2ProbesetidDict("RHEB").toList == correct)
  }


/**
  "removing by gene symbol" should "correctly discard annotations with that symbol" in {
    val newAnnotations = annotationsV2.removeBySymbol(Set("PSME1"))
    assert(newAnnotations.genes.forall(_.symbol.get.head != "PSME1"))
  }**/


  "removing by probesetID" should "correctly discard annotations with that probeset" in {
    val newAnnotations = annotationsV2.removeByProbeset(Set("201453_x_at"))
    assert(newAnnotations.genes.forall(_.probesetid != "201453_x_at"))
  }

  "removing by probesetID" should "correctly reset the indexing" in {
    val newAnnotations = annotationsV2.removeByProbeset(Set("201453_x_at"))
    assert(newAnnotations.index2ProbesetidDict(3) == "200059_s_at")
  }

  "indexing" should "be 1 based" in {
    assert(!annotationsV2.index2ProbesetidDict.keys.toList.contains(0))
  }

  "indexing" should "have only the sequential indices we expect" in {
    assert(annotationsV2.index2ProbesetidDict.keys.toArray.sorted.sameElements(Array(1, 2, 3, 4, 5)))
  }


}
