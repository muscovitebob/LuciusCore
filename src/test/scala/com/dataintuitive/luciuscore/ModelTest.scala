package com.dataintuitive.luciuscore

import com.dataintuitive.luciuscore.Model._

import org.scalatest.FlatSpec

/**
  * Created by toni on 15/06/16.
  */
class ModelTest extends FlatSpec {

  info("Test Experimental Model")

  val minSample = Sample("MinSample")
  val minCompound = Compound("MinCompound")
  val minSampleAnnotations = SampleAnnotations(minSample)
  val minCompoundAnnotations = CompoundAnnotations(minCompound)

  "Minimal Instantiation" should "simply work" in {
    assert(minSample.getPwid === "MinSample")
    assert(minCompound.getJnj === "MinCompound")
    assert(minSample.year === None)
    assert(minSample.getYear === "Sample year not available")
    assert(minCompound.smiles === None)
    assert(minSampleAnnotations.sample == minSample)
    assert(minCompoundAnnotations.compound == minCompound)
  }

  val sample1 = Sample(Some("sample1"), year=Some("2010"), concentration=Some("0.1"))
  val sample2 = Sample(Some("sample2"), year=Some("2012"), concentration=Some("0.2"))
  val compound1 = Compound(Some("compound1"), smiles=Some("43jbdhadfn3oure"), inchikey=Some("inchikey1"))
  val compound2 = Compound(Some("compound2"), smiles=Some("bdslkhjfadoi43d"), inchikey=Some("inchikey2"))

  val aRow = DbRow(Some("identity"), SampleAnnotations(
    minSample,
    Some(Array(2, 4, 5, 1, 6)),
    Some(Array(0.5, 0.05, 0.01, 0.2, 1)),
    Some(Array(2, 3, 4, 1, 5))),
    compoundAnnotations = minCompoundAnnotations)

  "dropping probesets" should "correctly return a new DbRow and recompute the ranks" in {
    val newRow = aRow.dropProbesetsByIndex(Set(2, 3))
    assert(newRow.sampleAnnotations.t.get.toList == List(2.0, 1.0, 6.0))
    assert(newRow.sampleAnnotations.p.get.toList == List(0.5, 0.2, 1.0))
    assert(newRow.sampleAnnotations.r.get.toList == List(2.0, 1.0, 3.0))
  }

}
