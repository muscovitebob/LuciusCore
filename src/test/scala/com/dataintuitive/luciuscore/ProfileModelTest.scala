package com.dataintuitive.luciuscore

import com.dataintuitive.luciuscore.Bing.GeneType
import com.dataintuitive.luciuscore.GeneModel.{GeneAnnotationV2, GenesV2}
import com.dataintuitive.luciuscore.Model.{Compound, CompoundAnnotations, DbRow, Sample, SampleAnnotations}
import com.dataintuitive.luciuscore.ProfileModel._
import com.dataintuitive.test.BaseSparkSessionSpec
import org.apache.spark.rdd.RDD
import org.scalatest.FlatSpec

class ProfileModelTest extends FlatSpec with BaseSparkSessionSpec {
  val annotationsV2 = new GenesV2(Array(
    new GeneAnnotationV2("200814_at", GeneType.Landmark, None, None, Some("PSME1"), None, None),
    new GeneAnnotationV2("222103_at", GeneType.Landmark, None, None, Some("ATF1"), None, None),
    new GeneAnnotationV2("201453_x_at", GeneType.Landmark, None, None, Some("RHEB"), None, None),
    new GeneAnnotationV2("200059_s_at", GeneType.Landmark, None, None, Some("RHOA"), None, None),
    new GeneAnnotationV2("200622_x_at", GeneType.Landmark, None, None, Some("CALM3"), None, None),
    new GeneAnnotationV2("220673_at", GeneType.Landmark, None, None, Some("ATF1"), None, None))
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

  it should "keep probesets desired" in {
    val keeplist = Set("200622_x_at")
    val newProfiles = profiles.keepProbesets(keeplist)
    assert(newProfiles.State.geneAnnotations.genes.map(_.probesetid).toList == List("200622_x_at"))
    assert(newProfiles.State.geneAnnotations.genes.map(_.symbol.get).toList == List("CALM3"))
  }

  "retrieveSignificant" should "correctly retrieve only indices with certain significance tresholds" in {

  }

}
