package com.dataintuitive.luciuscore.io

import com.dataintuitive.luciuscore.io.GenesIO._
import com.dataintuitive.test.BaseSparkContextSpec
import org.scalatest.FlatSpec

/**
  * Created by toni on 22/04/16.
  */
class GenesIOTest extends FlatSpec with BaseSparkContextSpec {

  info("Test loading of Gene annotations from file")

  val genes = loadGenesFromFile(sc, "src/test/resources/genesfile.tsv", "\t")

  "Loading gene data from a file" should "work" in {
    assert(genes.genes(0).ensemblid === "ens1")
  }


}

