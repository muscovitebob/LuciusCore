package com.dataintuitive.luciuscore.analytical

import Bing.GeneType
import com.dataintuitive.luciuscore.analytical.GeneAnnotations._
import com.dataintuitive.luciuscore.Model._
import com.dataintuitive.luciuscore.analytical.Signatures.SymbolSignatureV2
import com.dataintuitive.luciuscore.analytical.ProfileModel._
import com.dataintuitive.test.BaseSparkSessionSpec
import org.apache.spark.rdd.RDD
import org.scalatest.FlatSpec

class ProfileModelTest extends FlatSpec with BaseSparkSessionSpec {
  val annotationsV2 = new GeneAnnotationsDb(Array(
    new GeneAnnotationRecord("200814_at", Some(GeneType.Landmark), None, None, Some("PSME1"), None, None),
    new GeneAnnotationRecord("222103_at", Some(GeneType.Landmark), None, None, Some("ATF1"), None, None),
    new GeneAnnotationRecord("201453_x_at", Some(GeneType.Landmark), None, None, Some("RHEB"), None, None),
    new GeneAnnotationRecord("200059_s_at", Some(GeneType.Landmark), None, None, Some("RHOA"), None, None),
    new GeneAnnotationRecord("200622_x_at", Some(GeneType.Landmark), None, None, Some("CALM3"), None, None),
    new GeneAnnotationRecord("220673_at", Some(GeneType.Landmark), None, None, Some("ATF1"), None, None))
  )

  val row1 = DbRow(Some("GA666"), SampleAnnotations(
    Sample("sample1"),
    Some(Array(2, 4, 5, 1, 6, 7)), // t
    Some(Array(0.5, 0.05, 0.01, 0.2, 1, 0.02)), // p
    Some(Array(2, 3, 4, 1, 5, 6))), // r
    compoundAnnotations = CompoundAnnotations(Compound("aspirin")))

  val row2 = DbRow(Some("GA999"), SampleAnnotations(
    Sample("sample2"),
    Some(Array(4, 7, 9, 1, 3, 10)),
    Some(Array(0.05, 0.10, 0.02, 0.15, 0.001, 0.56)),
    Some(Array(3, 4, 5, 1, 2, 6))),
    compoundAnnotations = CompoundAnnotations(Compound("glyphosate")))
  val database: RDD[DbRow] = spark.sparkContext.parallelize(Array(row1, row2))

  val profiles = new ProfileDatabase(spark, database, annotationsV2)

  "profileDatabase" should "drop probesets and return a new, consistent, profiledatabase" in {
    val droplist = Set("201453_x_at")
    val newProfiles = profiles.dropProbesets(droplist)
    assert(newProfiles.WholeState.geneAnnotations.genes.map(_.probesetid).contains("201453_x_at") == false)
    assert(newProfiles.WholeState.database
      .map(row => row.sampleAnnotations)
      .flatMap(annots => List(annots.t.get.size, annots.p.get.size, annots.r.get.size))
      .filter(_ != 5).isEmpty())
    assert(newProfiles.WholeState.database
      .filter(_.pwid.get == "GA999")
      .map(row => row.sampleAnnotations.r.get)
      .collect.head.toList
      == List(3.0, 4.0, 1.0, 2.0, 5.0))
  }
  /**

  it should "drop gene symbols and a return a new, consistent, profiledatabase" in {
    val droplist = Set("RHEB")
    val newProfiles = profiles.dropGenes(droplist)
    assert(newProfiles.State.geneAnnotations.genes.map(_.probesetid).contains("201453_x_at") == false)
    assert(newProfiles.State.database
      .map(row => row.sampleAnnotations)
      .flatMap(annots => List(annots.t.get.size, annots.p.get.size, annots.r.get.size))
      .filter(_ != 5).isEmpty())
    assert(newProfiles.State.database
      .filter(_.pwid.get == "GA999")
      .map(row => row.sampleAnnotations.r.get)
      .collect.head.toList
      == List(3.0, 4.0, 1.0, 2.0, 5.0))
  }

  it should "keep gene symbols desired" in {
    val keeplist = Set("RHOA")
    val newProfiles = profiles.keepGenes(keeplist)
    assert(newProfiles.State.geneAnnotations.genes.map(_.probesetid).toList == List("200059_s_at"))
    assert(newProfiles.State.geneAnnotations.genes.map(_.symbol.get).toList == List("RHOA"))
  }

    **/

  it should "keep probesets desired" in {
    val keeplist = Set("200622_x_at")
    val newProfiles = profiles.keepProbesets(keeplist)
    assert(newProfiles.WholeState.geneAnnotations.genes.map(_.probesetid).toList == List("200622_x_at"))
    assert(newProfiles.WholeState.geneAnnotations.genes.map(_.symbol.get.toList).toList == List(List("CALM3")))
  }

  "retrieveSignificant" should "correctly retrieve only indices with certain significance thresholds" in {
    val sigThresh = 0.1
    val significant = profiles.retrieveSignificant(sigThresh)
    val justRows = significant.map(_._1).collect
    val justIndices = significant.map(_._2).collect
    assert(justRows(0).sampleAnnotations.t.get.toList == List(4, 5, 7))
    assert(justRows(1).sampleAnnotations.t.get.toList == List(4, 7, 9, 3))
    assert(justRows(0).sampleAnnotations.p.get.toList == List(0.05, 0.01, 0.02))
    assert(justRows(1).sampleAnnotations.p.get.toList == List(0.05, 0.10, 0.02, 0.001))
    assert(justRows(0).sampleAnnotations.r.get.toList == List(3, 4, 6.0))
    assert(justRows(1).sampleAnnotations.r.get.toList == List(3, 4, 5, 2))
    assert(justIndices(0).toList == List(2, 3, 6))
    assert(justIndices(1).toList == List(1, 2, 3, 5))

  }
  /**
  "zhangScore" should "correctly score the entire database against a signature" in {
    val signature = SymbolSignatureV2(Array("ATF1", "-PSME1"))
    val probesetsig = signature.translate2Probesetid(annotationsV2)
    val scores = profiles.zhangScore(probesetsig)
    assert(true)
  }**/

}
