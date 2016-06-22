package com.dataintuitive.luciuscore.Experiments

/**
  * Created by toni on 15/06/16.
  */

object ExperimentalModel extends Serializable {

  type Gene = String
  type Row = Array[Option[String]]

  case class DbRow(
                    val pwid: Option[String],
                    val sampleAnnotations: SampleAnnotations,
                    val compoundAnnotations: CompoundAnnotations
                  ) extends Serializable

  case class SampleAnnotations(
                                val sample: Sample,
                                val t: Option[Array[Double]] = None,
                                val p: Option[Array[Double]] = None,
                                val r: Option[Array[Double]] = None,
                                val z: Option[Array[Double]] = None
                              ) extends Serializable {

    def getSample = sample

    def tDefined = t.isDefined
    def pDefined = p.isDefined
    def rDefined = r.isDefined
    def zDefined = z.isDefined

  }

  // Moving Targets out of the way into seperate object!!!!
  case class Compound(
                       jnjs: Option[String],
                       jnjb: Option[String] = None,
                       smiles: Option[String] = None,
                       inchikey: Option[String] = None,
                       name: Option[String] = None,
                       ctype: Option[String] = None
                     ) extends Serializable {

    def getJnj = jnjs.getOrElse("Compound jnjs not available")
    def getJnjs = jnjs.getOrElse("Compound Jnjs not available")
    def getJnjb = jnjb.getOrElse("Compound Jnjb not available")
    def getSmiles = smiles.getOrElse("Compound smiles code not availalbe")
    def getInchikey = inchikey.getOrElse("Compound inchikey not available")
    def getName = name.getOrElse("Compound name not available")
    def getType = ctype.getOrElse("Compound type not availalbe")

  }

  object Compound {
    def apply(compoundString:String):Compound = Compound(Some(compoundString))
  }

  case class CompoundAnnotations(
                                  val compound: Compound,
                                  val knownTargets: Option[Set[Gene]] = None,
                                  val predictedTargets: Option[Set[Gene]] = None
                                ) extends Serializable {

    // Convenience method: usually jnjs is used as identifier
    def jnj = compound.jnjs

    // Map None to empty set as part of the high-level API
    def getKnownTargets = knownTargets.getOrElse(Set())
    def getPredictedTargets = predictedTargets.getOrElse(Set())
    def knownTargetsDefined = knownTargets.isDefined
    def predictedTargetsDefined = predictedTargets.isDefined

  }

  case class Sample(
                     val pwid: Option[String],
                     val batch: Option[String] = None,
                     val plateid: Option[String] = None,
                     val well: Option[String] = None,
                     val protocolname: Option[String] = None,
                     val concentration: Option[String] = None,
                     val year: Option[String] = None
                   ) extends Serializable {

    def getPwid = pwid.getOrElse("Sample pwid not available")
    def getBatch = batch.getOrElse("Sample batch not available")
    def getPlateid = plateid.getOrElse("Sample plateid not available")
    def getWell = well.getOrElse("Sample well not available")
    def getProtocolname = protocolname.getOrElse("Sample protocolname not available")
    def getConcentration = concentration.getOrElse("Sample concentration not available")
    def getYear = year.getOrElse("Sample year not available")

  }

  object Sample {
    def apply(sampleString:String):Sample = Sample(Some(sampleString))
  }

}