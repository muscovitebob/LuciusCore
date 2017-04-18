package com.dataintuitive.luciuscore.io

import com.dataintuitive.luciuscore.Model._
import ParseFunctions._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * The code needed to load the data from _disk_.
  */
object SampleCompoundRelationsIO extends Serializable {

  def parseNA(s: Option[String]):Option[String] = s match {
    case Some("NA") => None
    case _ => s
  }

  /**
    * Function to load data in V3 format from a file. This is the format used from December 2016 onwards.
    */
  def loadSampleCompoundRelationsFromFileV3(sc: SparkContext, fileName: String): RDD[DbRow] = {

    val data = sc.textFile(fileName).map(_.split("\t"))

    val featuresToExtract = Seq("sampleID",      // 0
                                "JNJS",          // 1
                                "JNJB",          // 2
                                "Batch",         // 3
                                "plateID",       // 4
                                "well",          // 5
                                "cellLine",      // 6
                                "Concentration", // 7
                                "year",          // 8
                                "name")          // 9

    extractFeatures(data, features = featuresToExtract, includeHeader = false)
      .map { r =>
        // Strip -AAA from the compound jnj number for compatibility
        val thisCompound = Compound(jnjs = parseNA(r(1)).map(_.stripSuffix("-AAA")),
                                    name = parseNA(r(9)))
        val thisSample = Sample(pwid = r(0),
                                batch = r(3),
                                plateid = r(4),
                                well = r(5),
                                protocolname = r(6),
                                concentration = r(7),
                                year = r(8))
        val thisCompoundAnnotations = CompoundAnnotations(thisCompound)
        val thisSampleAnnotations = SampleAnnotations(thisSample)
        DbRow(r(0), thisSampleAnnotations, thisCompoundAnnotations)
      }.cache
  }

  /**
    * Function to load data in V2 format from a file.
    */
  def loadSampleCompoundRelationsFromFileV2(sc: SparkContext, fileName: String): RDD[DbRow] = {

    val data = sc.textFile(fileName).map(_.split("\t"))

    val featuresToExtract = Seq("sampleID",
                                "JNJS",
                                "batchL1000",
                                "plateID",
                                "well",
                                "cellLine",
                                "Concentration",
                                "year")

    extractFeatures(data, features = featuresToExtract, includeHeader = false)
      .map { r =>
        // Strip -AAA from the compound jnj number for compatibility
        val thisCompound = Compound(jnjs = r(1).map(_.stripSuffix("-AAA")))
        val thisSample = Sample(pwid = r(0),
                                batch = r(2),
                                plateid = r(3),
                                well = r(4),
                                protocolname = r(5),
                                concentration = r(6),
                                year = r(7))
        val thisCompoundAnnotations = CompoundAnnotations(thisCompound)
        val thisSampleAnnotations = SampleAnnotations(thisSample)
        DbRow(r(0), thisSampleAnnotations, thisCompoundAnnotations)
      }.cache
  }

  /**
    * Function to load data in V1 format from a file. For V1, the compound annotations are in the relations file.
    *
    * Remark: The header is important, and case-sensitive!
    */
  def loadSampleCompoundRelationsFromFileV1(sc: SparkContext, fileName: String): RDD[DbRow] = {

    val data = sc.textFile(fileName).map(_.split("\t").map(_.trim))

    val featuresToExtract = Seq("plateWellId",
                                "JNJS",
                                "JNJB",
                                "batch",
                                "plateID",
                                "well",
                                "protocolName",
                                "concentration",
                                "year",
                                "SMILES",
                                "InChIKey",
                                "compoundName",
                                "type",
                                "targets")

    extractFeatures(data, features = featuresToExtract, includeHeader = false)
      .map { r =>
        // Strip -AAA from the compound jnj number for compatibility
        val thisCompound = Compound(jnjs = r(1).map(_.stripSuffix("-AAA")),
                                    jnjb = r(2),
                                    smiles = r(9),
                                    name = r(11),
                                    inchikey = r(10),
                                    ctype = r(12))
        val thisKnownTargets = r(13).map{
          targetsString => targetsString.split(",").map(_.trim).toSet
        }
        val thisSample = Sample(pwid = r(0),
                                batch = r(3),
                                plateid = r(4),
                                well = r(5),
                                protocolname = r(6),
                                concentration = r(7),
                                year = r(8))
        val thisCompoundAnnotations = CompoundAnnotations(thisCompound, knownTargets = thisKnownTargets)
        val thisSampleAnnotations = SampleAnnotations(thisSample)
        DbRow(r(0), thisSampleAnnotations, thisCompoundAnnotations)
      }.cache
  }

}
