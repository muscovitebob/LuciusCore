package com.dataintuitive.luciuscore.analytical.io

import com.dataintuitive.luciuscore.analytical.Bing.GeneType
import com.dataintuitive.luciuscore.analytical.io.GeneAnnotationsIO._
import org.scalatest.{FlatSpec, Matchers}
import com.dataintuitive.test.BaseSparkSessionSpec

class GeneAnnotationsIOTest extends FlatSpec with BaseSparkSessionSpec {

  val genesV2 =
    loadGeneAnnotationsFromFile(spark, "src/test/resources/geneAnnotationsV2.txt", "\t", "main/resources/BING.csv")

  "Loading V2 gene data" should "work" in {
    assert(genesV2.genes(0).symbol.get == "PSME1")
  }

  "Loading V2 gene data" should "generate None for '---' fields" in {
    assert(genesV2.genes(0).entrezid == None)
  }

  "Loading V2 gene data" should "generate None for empty fields" in {
    assert(genesV2.genes(0).geneFamily == None)
  }

  "Loading V2 gene data" should "automatically annotate the inference type" in {
    assert(genesV2.genes(0).dataType == GeneType.Landmark)
  }

}
