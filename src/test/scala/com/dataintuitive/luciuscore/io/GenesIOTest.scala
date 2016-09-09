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

  val genes = loadGenesFromFile(sc, "src/test/resources/genesfile.tsv", "\t")

  "Loading gene data from a file" should "work" in {
    assert(genes.genes(0).ensemblid === "ens1")
  }

  info("Test loading of Gene annotations with wrong number of features")

  def genesWithWrongFeatures = loadGenesFromFile(sc,
                                "src/test/resources/genesfile.tsv",
                                features = Seq("probesetid", "entrezid", "missing"),
                                delimiter = "\t")

  "Loading gene data with wrong number of features" should "should thrown exception" in {

    val thrown = the [java.lang.IllegalArgumentException] thrownBy (genesWithWrongFeatures)

    thrown should have message "requirement failed: The length of the features vector needs to 5"

  }

  info("Test loading of Gene annotations with missing values")

  // Please note that we imitate missing values by selecting a non-existing column from the file
  val genesWithMissingFeatures = loadGenesFromFile(sc,
                                      "src/test/resources/genesfile.tsv",
                                      features = Seq("probesetid", "entrezid", "missing", "missing", "missing"),
                                      delimiter = "\t")

  "Loading gene data from a file with missing data" should "work and convert to NA" in {
    assert(genesWithMissingFeatures.genes(0).ensemblid === "NA")
  }

}

