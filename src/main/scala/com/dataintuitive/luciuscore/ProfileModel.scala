package com.dataintuitive.luciuscore

import com.dataintuitive.luciuscore.GeneModel.GenesV2
import com.dataintuitive.luciuscore.Model._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import com.dataintuitive.luciuscore.DbFunctions._
import com.dataintuitive.luciuscore.SignatureModel._
import com.dataintuitive.luciuscore.TransformationFunctions._

object ProfileModel {
  /**
    * Object to deal with RDD[DbRow] AKA databases of transcriptomic profiles, against which connectivity scoring is possible
    */

  class ProfileDatabase(spark: SparkSession, database: RDD[DbRow], geneAnnotations: GenesV2) extends Serializable {
    // check consistency
    require(database.map(_.sampleAnnotations.t.get.length).filter(_ != geneAnnotations.genes.length).isEmpty)
    require(database.map(_.sampleAnnotations.p.get.length).filter(_ != geneAnnotations.genes.length).isEmpty)
    require(database.map(_.sampleAnnotations.r.get.length).filter(_ != geneAnnotations.genes.length).isEmpty)

    case class ProfileDbState(database: RDD[DbRow], geneAnnotations: GenesV2)
    val State = ProfileDbState(database, geneAnnotations)
    val RankedState = ProfileDbState(database.filter(x => x.sampleAnnotations.r.isDefined), geneAnnotations)

    def dropGenes(droplist: Set[Symbol]): ProfileDatabase = {
      val probesetsToDrop = droplist.flatMap(symbol => geneAnnotations.symbol2ProbesetidDict(Some(symbol)))
      val indicesToDrop = probesetsToDrop.map(probeset => geneAnnotations.probesetid2IndexDict(probeset))
      val droppedDatabase = database.map(x => DbRow.dropProbesetsByIndex(x, indicesToDrop))
      val droppedGeneAnnotations = geneAnnotations.removeBySymbol(droplist)
      new ProfileDatabase(this.spark, droppedDatabase, droppedGeneAnnotations)
    }

    def dropProbesets(droplist: Set[Probesetid]): ProfileDatabase = {
      val indicesToDrop = droplist.map(probeset => geneAnnotations.probesetid2IndexDict(probeset))
      val droppedDatabase = database.map(x => DbRow.dropProbesetsByIndex(x, indicesToDrop))
      val droppedGeneAnnotations = geneAnnotations.removeByProbeset(droplist)
      new ProfileDatabase(this.spark, droppedDatabase, droppedGeneAnnotations)
    }

    def retrieveSignificant(database: RDD[DbRow], significanceLevel: Double = 0.5): RDD[DbRow] = {
      val significantIndices = database.map(row => (row.pwid, row.sampleAnnotations))
        .map(taggedannot => (taggedannot._1,
          taggedannot._2.p.get.zip(Stream from 1).filter(_._1 <= significanceLevel).map(_._2))).collect.toMap
      database.map(row => (row, significantIndices(row.pwid).toSet))
        .map(tuple => DbRow.dropProbesetsByIndex(tuple._1, tuple._2, rerank = false))
    }

    def zhangScore(signature: List[String],
                   significantOnly: Boolean = true, significanceLevel: Double = 0.05): Map[Sample, Double] = {
      require(significanceLevel >= 0 && significanceLevel <= 1)
      // first leave only the probesets of interest
      val localData = ProfileDatabase.this.dropGenes(signature.toSet)
      // give the signature ranks
      val signatureFormal = SymbolSignature(signature.toArray).translate2Probesetid()
      val sigRanks = signature2OrderedRankVector(signature, signature.length)
      // now subset by significance
      val significantLocalDatabase = if (significantOnly) {
        val significantDbRows = retrieveSignificant(localData.database, significanceLevel)
        val scored = significantDbRows.map(row => row)
        ???





      }
    ???
    }





  }
}
