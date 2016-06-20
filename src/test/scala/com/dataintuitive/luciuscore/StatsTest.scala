package com.dataintuitive.luciuscore

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FlatSpec
import com.dataintuitive.test.BaseSparkContextSpec

/**
  * Created by toni on 22/04/16.
  */
class StatsTest extends FlatSpec with BaseSparkContextSpec {

  info("Test Stats class, loading and parsing of data")

  val data = Stats(sc, "src/test/resources/statsfile.tsv", "\t")

  // Take into account the data is transposed!
  "Loading test data from a file" should "work and result in correct object" in {
    assert(data.features === Array("value1", "value2", "value3", "value4"))
    assert(data.samples === Array("first", "second", "third", "fourth"))
    assert(data.asRdd.first._2 === Array(1.2, 1.2, 1.9, 1.2))
  }

  // Take into account the data is transposed!
  "A simple map/reduce over the stats Rdd" should "work" in {
    val mapreduce = data.asRdd.map(_._2(0)).reduce(_+_)
    assert(mapreduce === 11.3)
  }

  // Take into account the data is transposed!
  "Empty values or NA" should "be parsed and mapped onto a default value" in {
//    val dataAsArray = data.asRdd.collect()
//    assert(dataAsArray(1)(1) === 0.0)
//    assert(dataAsArray(2)(3) === 0.0)
  }

  // Read the same datafile again
  val data2 = Stats(sc, "src/test/resources/statsfile.tsv", "\t")

  // Take into account the data is transposed!
  "Loading the same dataset twice" should "result in the same result" in {
    assert(data2.features === Array("value1", "value2", "value3", "value4"))
    assert(data2.samples === Array("first", "second", "third", "fourth"))
    assert(data2.asRdd.first._2 === Array(1.2, 1.2, 1.9, 1.2))
  }

  // Be careful, after joining the compounds are not necessarily in the same order!
  "Use the constructor to join two stats" should "work and result in twoD stats" in {
    val joined = data.addP(data2)
    assert(joined.asRddKV.lookup("first")(0)._1 === data.asRddKV.lookup("first")(0))
  }

  val tprStats = data.addP(sc, "src/test/resources/statsfile.tsv", "\t").addRanks

  // Adding the ranks automatically
  // Be careful, after joining the compounds are not necessarily in the same order!
  "Extend the TP stats with ranks" should "work" in {
    assert(tprStats.rdd.first.pwid === "second")
  }

  // The pwids don't match between the files, so have to look back to this !
  val relations = SampleCompoundRelations(sc, "src/test/resources/samplecompoundrelations.tsv", "\t")
  val atprStats = tprStats.addAnnotations(relations)

  // Joining the annotations
  // Be careful, after joining the compounds are not necessarily in the same order!
//  "Extend the TPR stats with annotations" should "work" in {
//    assert(atprStats.rdd.first.pwid === List())
//  }

}
