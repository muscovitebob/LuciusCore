package com.dataintuitive.luciuscore

import com.dataintuitive.luciuscore.TransformationFunctions._

/**
  * The types and classes used throughout Lucius.
  *
  * Be aware: The gene-model and signature model are not included here.
  */
object Model extends Serializable {

  // For Vector derivatives: A ValueVector is for t-stats and p-stats
  type Value = Double
  type ValueVector = Array[Value]

  // A RankVector contains ranks.
  type Rank = Double
  type RankVector = Array[Rank]

  // A GeneVector is an ordered list of Genes (aka signature)
  type Gene = String
  type GeneVector = Array[Gene]

  type Probesetid = String
  type Symbol = String

  type GeneDictionary = Map[Symbol, Probesetid]
  type InverseGeneDictionary = Map[Probesetid,Symbol]

  type GeneDictionaryV2 = Map[Option[Symbol], Array[Probesetid]]
  type InverseGeneDictionaryV2 = Map[Probesetid, Option[Symbol]]

  type NotationType = String
  val SYMBOL = "symbol"
  val PROBESETID = "probesetid"
  val INDEX = "index"

  type SignatureType = GeneVector

  type Index = Int

  type Row = Array[Option[String]]

  case class DbRow(pwid: Option[String],
                   sampleAnnotations: SampleAnnotations,
                   compoundAnnotations: CompoundAnnotations) extends Serializable

  object DbRow {

    private def removeByIndex(indices: Set[Int], collection: Array[Double]): Array[Double] = {
      collection.zip(Stream from 1).filter { case (x, i) => !indices.contains(i) }.map(_._1)
    }

    def dropProbesetsByIndex(thisRow: DbRow, indices: Set[Int], rerank: Boolean = true): DbRow = {
      if (thisRow.sampleAnnotations.t.isDefined &&
        thisRow.sampleAnnotations.p.isDefined) {
        val newSampleAnnotation = thisRow.sampleAnnotations.copy(
          t = thisRow.sampleAnnotations.t.map(removeByIndex(indices, _)),
          p = thisRow.sampleAnnotations.p.map(removeByIndex(indices, _))
        )
        val newRankedSampleAnnotation = if (rerank) { newSampleAnnotation.copy(
          r = Some(stats2RankVector((newSampleAnnotation.t.get, newSampleAnnotation.p.get)))
        )
        } else {
          newSampleAnnotation.copy(r = newSampleAnnotation.r.map(removeByIndex(indices, _)))
        }
        thisRow.copy(sampleAnnotations = newRankedSampleAnnotation)
      } else thisRow
    }
  }


  case class SampleAnnotations(
                                val sample: Sample,
                                val t: Option[Array[Double]] = None,
                                val p: Option[Array[Double]] = None,
                                val r: Option[Array[Double]] = None
                              ) extends Serializable {

    def getSample = sample

    def tDefined = t.isDefined
    def pDefined = p.isDefined
    def rDefined = r.isDefined

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
    def apply(compoundString:String): Compound = Compound(Some(compoundString))
  }

  case class CompoundAnnotations(
                       val compound: Compound,
                       val knownTargets: Option[Seq[Gene]] = None,
                       val predictedTargets: Option[Seq[Gene]] = None
                      ) extends Serializable {

    // Convenience method: usually jnjs is used as identifier
    def jnj = compound.jnjs

    // Map None to empty set as part of the high-level API
    def getKnownTargets = knownTargets.getOrElse(Seq())
    def getPredictedTargets = predictedTargets.getOrElse(Seq())
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
    def apply(sampleString: String): Sample = Sample(Some(sampleString))
  }

}