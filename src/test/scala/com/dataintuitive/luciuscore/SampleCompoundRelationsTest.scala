package com.dataintuitive.luciuscore

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FlatSpec
import com.dataintuitive.test.BaseSparkContextSpec

/**
  * Created by toni on 23/04/16.
  */
class SampleCompoundRelationsTest extends FlatSpec with BaseSparkContextSpec {

  info("Test SampleCompoundRelations class, loading and parsing of data")

  val relations = SampleCompoundRelations(sc, "src/test/resources/samplecompoundrelations.tsv", "\t")

  // Take into account the data is transposed!
  "Loading test data from a file" should "work and result in correct object" in {
    assert(relations.asRdd.first.sample.samplePwid === "pwid1")
    assert(relations.asRdd.first.compound.compoundSmiles === "smiles1")
  }

  "Two different samples" should "be related to the same compound" in {
    val compound =
      relations.asRdd.filter(x => x.sample.samplePwid == "pwid1" || x.sample.samplePwid == "pwid2")
        .map(x => (x.compound.compoundJnjs, 1))
        .reduceByKey(_ + _)
        .keys
        .collect()
    assert(compound === Array("jnjs1"))
  }

  it should "work using the dictionary as well" in {
    assert(relations.dictionaryRdd.map(_.swap).lookup("jnjs1") === Array("pwid1","pwid2"))
    assert(relations.inverseDictionaryRdd.lookup("jnjs1") === Array("pwid1","pwid2"))
  }
}
