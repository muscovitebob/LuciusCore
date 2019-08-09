package com.dataintuitive.luciuscore.analytical

import com.dataintuitive.luciuscore.analytical.GeneAnnotations.GeneAnnotationsDb
import com.dataintuitive.luciuscore.Model.{DbRow, Probesetid, Sample, Symbol}
import com.dataintuitive.luciuscore.analytical.Signatures.{ProbesetidSignatureV2, SymbolSignatureV2}
import com.dataintuitive.luciuscore.ZhangScoreFunctions.connectionScore
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ProfileModel {
  /**
    * Object to deal with databases of transcriptomic profiles, against which connectivity scoring is possible
    * bundling annotations with the profile RDD guarantees consistent interpretation
    * BIG ASSUMPTION: The original geneAnnotations.txt that was loaded and the statistics files
    * had the same probeset ordering.
    */

  class ProfileDatabase(spark: SparkSession, database: RDD[DbRow], geneAnnotations: GeneAnnotationsDb) extends Serializable {
    // check consistency
    require(database.map(_.sampleAnnotations.t.get.length).filter(_ != geneAnnotations.genes.length).isEmpty)
    require(database.map(_.sampleAnnotations.p.get.length).filter(_ != geneAnnotations.genes.length).isEmpty)
    require(database.map(_.sampleAnnotations.r.get.length).filter(_ != geneAnnotations.genes.length).isEmpty)

    case class ProfileDbState(database: RDD[DbRow], geneAnnotations: GeneAnnotationsDb)
    val State = ProfileDbState(database, geneAnnotations)
    val RankedState = ProfileDbState(database.filter(x => x.sampleAnnotations.r.isDefined), geneAnnotations)

    def dropGenes(droplist: Set[Symbol]): ProfileDatabase = {
      require(droplist.forall(symbol => geneAnnotations.symbol2ProbesetidDict.contains(Some(symbol))))

      val probesetsToDrop = droplist.flatMap(symbol => geneAnnotations.symbol2ProbesetidDict(Some(symbol)))
      val indicesToDrop = probesetsToDrop.map(probeset => geneAnnotations.probesetid2IndexDict(probeset))
      val droppedDatabase = database.map(x => x.dropProbesetsByIndex(indicesToDrop))
      val droppedGeneAnnotations = geneAnnotations.removeBySymbol(droplist)
      new ProfileDatabase(this.spark, droppedDatabase, droppedGeneAnnotations)
    }

    def dropProbesets(droplist: Set[Probesetid]): ProfileDatabase = {
      require(droplist.forall(probeset => geneAnnotations.probesetid2SymbolDict.contains(probeset)))

      val indicesToDrop = droplist.map(probeset => geneAnnotations.probesetid2IndexDict(probeset))
      val droppedDatabase = database.map(x => x.dropProbesetsByIndex(indicesToDrop))
      val droppedGeneAnnotations = geneAnnotations.removeByProbeset(droplist)
      new ProfileDatabase(this.spark, droppedDatabase, droppedGeneAnnotations)
    }

    def keepGenes(keeplist: Set[Symbol]): ProfileDatabase = {
      require(keeplist.forall(symbol => geneAnnotations.symbol2ProbesetidDict.contains(Some(symbol))))

      val probesetsToKeep = keeplist.flatMap(symbol => geneAnnotations.symbol2ProbesetidDict(Some(symbol)))
      val indicesToKeep = probesetsToKeep.map(probeset => geneAnnotations.probesetid2IndexDict(probeset))
      val indicesToDrop = geneAnnotations.index2ProbesetidDict.keySet diff indicesToKeep
      val droplist = geneAnnotations.symbol2ProbesetidDict.keySet.map(_.get) diff keeplist

      val droppedDatabase = database.map(x => x.dropProbesetsByIndex(indicesToDrop))
      val droppedGeneAnnotations = geneAnnotations.removeBySymbol(droplist)
      new ProfileDatabase(this.spark, droppedDatabase, droppedGeneAnnotations)

    }

    def keepProbesets(keeplist : Set[Probesetid]): ProfileDatabase = {
      require(keeplist.forall(probeset => geneAnnotations.probesetid2SymbolDict.contains(probeset)))

      val indicesToKeep = keeplist.map(probeset => geneAnnotations.probesetid2IndexDict(probeset))
      val indicesToDrop = geneAnnotations.index2ProbesetidDict.keySet diff indicesToKeep
      val droplist = geneAnnotations.probesetid2IndexDict.keySet diff keeplist

      val droppedDatabase = database.map(x => x.dropProbesetsByIndex(indicesToDrop))
      val droppedGeneAnnotations = geneAnnotations.removeByProbeset(droplist)

      new ProfileDatabase(this.spark, droppedDatabase, droppedGeneAnnotations)
    }

    /**
      * DbRows with only the significant entires for t, p and r. r is not re-ranked. Each DbRow includes Array of
      * significant indices we subset for tracing back to original annotation
      * @param significanceLevel
      * @return
      */
    def retrieveSignificant(significanceLevel: Double = 0.5): RDD[(DbRow, Array[Int])] = {
      require(significanceLevel >= 0 && significanceLevel <= 1)
      // give p vals one based index, filter out all more than siglevel, then drop each DbRows corresponding indices
      val significantIndices = database.map(row => (row.pwid, row.sampleAnnotations))
        .map(taggedannot => (taggedannot._1, taggedannot._2.p.get.zip(Stream from 1).filter(_._1 <= significanceLevel)))
        .collect.toMap

      val nonSignificantIndices = significantIndices.map(x => (x._1, x._2.map(_._2).toSet))
        .map(x => (x._1, geneAnnotations.index2ProbesetidDict.keySet diff x._2)).toMap

      val filteredDb = database.map(row => (row, nonSignificantIndices(row.pwid)))
        .map(tuple => tuple._1.dropProbesetsByIndex(tuple._2, rerank = false))

      filteredDb.map(x => (x, significantIndices(x.pwid).map(_._2)))
    }


    def zhangScore(aSignature: SymbolSignatureV2,
                   significantOnly: Boolean = true, significanceLevel: Double = 0.05, bingOnly: Boolean = true): Map[Sample, (Double, ProbesetidSignatureV2)] = {
      require(significanceLevel >= 0 && significanceLevel <= 1)
      // first leave only the probesets of interest
      val localData = ProfileDatabase.this.keepGenes(aSignature.values.toSet).RankedState
      val probesetSig = aSignature.translate2Probesetid(localData.geneAnnotations)
      // now subset by significance
      val scores = if (significantOnly) {
        val significant = retrieveSignificant(significanceLevel)
        val significantDbRows = significant.map(_._1)
        val pwidAndIndices = significant.map(x => (x._1.pwid, x._2))
        val scored = significantDbRows.map(row => (row.sampleAnnotations.sample, (connectionScore(row.sampleAnnotations.r.get, probesetSig.r), probesetSig))).collect.toMap
        scored
      } else {
        val scored = localData.database.map(x => (x.sampleAnnotations.sample, (connectionScore(x.sampleAnnotations.r.get, probesetSig.r), probesetSig))).collect.toMap
        scored
      }
      scores
    }





    }





}
