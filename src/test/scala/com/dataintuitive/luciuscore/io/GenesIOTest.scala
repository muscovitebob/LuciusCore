package com.dataintuitive.luciuscore.io

import com.dataintuitive.luciuscore.io.GenesIO._
import com.dataintuitive.test.BaseSparkContextSpec
import org.scalatest.FlatSpec
import org.scalatest.Matchers

/**
  * Created by toni on 22/04/16.
  */
class GenesIOTest extends FlatSpec with BaseSparkContextSpec with Matchers {

  info("Test loading of Gene annotations from file")

  val genes = loadGenesFromFile(sc, "src/test/resources/geneAnnotations.txt", "\t")

  "Loading gene data from a file" should "work" in {
    assert(genes.genes(0).symbol === "PSME1")
  }

  info("Test loading of Gene annotations with wrong number of features")

  def genesWithWrongFeatures = loadGenesFromFile(sc,
                                "src/test/resources/geneAnnotations.txt",
                                delimiter = "\t")

  info("Test loading of Gene annotations with missing values")

  // Please note that we imitate missing values by selecting a non-existing column from the file
  val genesWithMissingFeatures = loadGenesFromFile(sc,
                                      "src/test/resources/geneAnnotations.txt",
                                      delimiter = "\t")

  "Loading gene data from a file with missing data" should "work and convert to NA" in {
    assert(genesWithMissingFeatures.genes(0).ensemblid === "NA")
  }

  info("Test gene annotation loading in V2 format")

  val genesV2 = loadGenesFromFileV2(sc, "src/test/resources/geneAnnotationsV2.txt")

  "Loading V2 gene data" should "work" in {
    assert(genesV2.genes(0).symbol.get == "PSME1")
  }

  "Loading V2 gene data" should "generate None for '---' fields" in {
    assert(genesV2.genes(0).entrezid == None)
  }

  "Loading V2 gene data" should "generate None for empty fields" in {
    assert(genesV2.genes(0).geneFamily == None)
  }

}

