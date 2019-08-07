package com.dataintuitive.luciuscore

import com.dataintuitive.luciuscore.Bing.GeneType
import com.dataintuitive.luciuscore.GeneModel._
import com.dataintuitive.luciuscore.Bing.GeneType.GeneType
import org.scalatest.FlatSpec

/**
  * Created by toni on 22/04/16.
  */
class GeneModelTest extends FlatSpec {

  info("Test model for gene annotations")

  val gene: GeneAnnotation = new GeneAnnotation("probesetidString",
    "entrezidString",
    "ensemblidString",
    "symbolString",
    "nameString")

  "methods on a gene" should "return the method field" in {
    assert(gene.name === "nameString")
    assert(gene.symbol === "symbolString")
    assert(gene.ensemblid === "ensemblidString")
    assert(gene.entrezid === "entrezidString")
    assert(gene.probesetid === "probesetidString")
  }

  info("Test model for gene annotations V2")

  val geneV2: GeneAnnotationV2 = new GeneAnnotationV2("probesetidString",
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
    assert(geneV2.dataType === GeneType.Landmark)
  }

  info("Test GenesV2 annotations container")

  val annotationsV2 = new GenesV2(Array(
    new GeneAnnotationV2("200814_at", GeneType.Landmark, None, None, Some("PSME1"), None, None),
    new GeneAnnotationV2("222103_at", GeneType.Landmark, None, None, Some("ATF1"), None, None),
    new GeneAnnotationV2("201453_x_at", GeneType.Landmark, None, None, Some("RHEB"), None, None),
    new GeneAnnotationV2("200059_s_at", GeneType.Landmark, None, None, Some("RHOA"), None, None),
    new GeneAnnotationV2("220034_at", GeneType.Landmark, None, None, Some("RHEB"), None, None)
  ))

  "GenesV2" should "instantiate as a database of gene annotations correctly" in {
    assert(annotationsV2.genes.filter(_.symbol.get == "PSME1").head.probesetid == "200814_at")
  }

  "GenesV2" should "instantiate the symbol -> probeset dict correctly" in {
    val correct = Array("201453_x_at", "220034_at")
    assert(annotationsV2.symbol2ProbesetidDict(Some("RHEB")).sameElements(correct))
  }

  "removing by gene symbol" should "correctly discard annotations with that symbol" in {
    val newAnnotations = annotationsV2.removeBySymbol(Set("PSME1"))
    assert(newAnnotations.genes.forall(_.symbol.get != "PSME1"))
  }


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

