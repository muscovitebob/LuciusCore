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


}

