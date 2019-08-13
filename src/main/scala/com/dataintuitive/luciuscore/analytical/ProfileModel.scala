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
    // we can only perform analysis on the subset of DbRows that have t statistics provided

    case class ProfileDbState(database: RDD[DbRow], geneAnnotations: GeneAnnotationsDb)
    val WholeState = ProfileDbState(database, geneAnnotations)
    val AnalysableState = ProfileDbState(database.filter(x => x.sampleAnnotations.r.isDefined)
      .filter(x => x.sampleAnnotations.t.isDefined)
      .filter(x => x.sampleAnnotations.p.isDefined), geneAnnotations)
    // check consistency
    require(AnalysableState.database.map(_.sampleAnnotations.t.get.length).filter(_ != geneAnnotations.genes.length).isEmpty)

    val unAnalysableSamples = WholeState.database.filter(x => x.sampleAnnotations.t.isEmpty).map(x => x.pwid)
    val analysableSamples = WholeState.database.filter(x => x.sampleAnnotations.t.isDefined).map(x => x.pwid)


    /**
    def dropGenes(droplist: Set[Symbol]): ProfileDatabase = {
      require(droplist.forall(symbol => geneAnnotations.symbol2ProbesetidDict.contains(symbol)))

      val probesets = droplist.map(symbol => (symbol, geneAnnotations.symbol2ProbesetidDict.get(symbol)))
      // if a gene symbol doesnt have probesets we dont drop anything for it
      val probesetsToDrop = probesets.filter(x => x._2.isDefined).map(x => (x._1, x._2.get))

      val indicesToDrop = probesetsToDrop.map(symbol => (symbol._1, symbol._2.map(geneAnnotations.probesetid2IndexDict)))
      val droppedDatabase = database.map(x => x.dropProbesetsByIndex(indicesToDrop.flatMap(_._2)))
      val droppedGeneAnnotations = geneAnnotations.removeBySymbol(droplist)
      new ProfileDatabase(this.spark, droppedDatabase, droppedGeneAnnotations)
    }
      **/
    def dropProbesets(droplist: Set[Probesetid]): ProfileDatabase = {
      require(droplist.forall(probeset => geneAnnotations.probesetid2SymbolDict.contains(probeset)))

      val indicesToDrop = droplist.map(probeset => geneAnnotations.probesetid2IndexDict(probeset))
      val droppedDatabase = database.map(x => x.dropProbesetsByIndex(indicesToDrop))
      val droppedGeneAnnotations = geneAnnotations.removeByProbeset(droplist)
      new ProfileDatabase(this.spark, droppedDatabase, droppedGeneAnnotations)
    }


    /**
    def keepGenes(keeplist: Set[Symbol]): ProfileDatabase = {
      require(keeplist.forall(symbol => geneAnnotations.symbol2ProbesetidDict.contains(symbol)))

      val probesets = keeplist.map(symbol => (symbol, geneAnnotations.symbol2ProbesetidDict.get(symbol)))
      // if a gene doesnt have probesets we drop it
      val probesetsToKeep = probesets.filter(x => x._2.isDefined).map(x => (x._1, x._2.get))
      val indicesToKeep = probesetsToKeep.map(symbol => (symbol._1, symbol._2.map(geneAnnotations.probesetid2IndexDict)))
      val indicesToDrop = geneAnnotations.index2ProbesetidDict.keySet diff indicesToKeep.flatMap(_._2)
      val droplist = geneAnnotations.symbol2ProbesetidDict.keySet diff keeplist

      val droppedDatabase = database.map(x => x.dropProbesetsByIndex(indicesToDrop))
      val huh1 = droppedDatabase.collect
      val droppedGeneAnnotations = geneAnnotations.removeBySymbol(droplist)
      new ProfileDatabase(this.spark, droppedDatabase, droppedGeneAnnotations)

    }**/

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
        .map(taggedannot => (taggedannot._1, taggedannot._2.p.getOrElse(Array(0.0)).zip(Stream from 1).filter(_._1 <= significanceLevel)))

      val nonSignificantIndices = significantIndices.map(x => (x._1, x._2.map(_._2).toSet))
        .map(x => (x._1, geneAnnotations.index2ProbesetidDict.keySet diff x._2))

      //val filteredDbOne = AnalysableState.database.map(row => (row, nonSignificantIndices.lookup(row.pwid)))

      val filteredDbOne = AnalysableState.database.keyBy(x => x.pwid)

      val joinedOne = filteredDbOne.join(nonSignificantIndices).map{case (k, v) => v}

      val filteredDbTwo = joinedOne.map(tuple => tuple._1.dropProbesetsByIndex(tuple._2, rerank = false))

      val filteredDbThree = filteredDbTwo.keyBy(x => x.pwid)

      val joinedTwo = filteredDbThree.join(significantIndices).map{case (k, v) => v}.map(x => (x._1, x._2.map(_._2)))

      joinedTwo
    }

    def retrieveBING: RDD[(DbRow, Array[Int])] = {
      ???
    }


    def zhangScore(aSignature: ProbesetidSignatureV2,
                   significantOnly: Boolean = true, significanceLevel: Double = 0.05, bingOnly: Boolean = true): RDD[(Option[String], ProbesetidSignatureV2, Double)] = {
      require(significanceLevel >= 0 && significanceLevel <= 1)
      // first leave only the probesets of interest
      val localData = ProfileDatabase.this.keepProbesets(aSignature.values.toSet).AnalysableState
      // now subset by significance
      val remainingDbRows = if (significantOnly) {
        val significant = retrieveSignificant(significanceLevel)
        val significantDbRows = significant.map(_._1)
        val pwidAndIndices = significant.map(x => (x._1.pwid, x._2))
        significantDbRows
      } else {localData.database}

      val scored = remainingDbRows.map(
        row => (row.pwid, aSignature,
          if (row.sampleAnnotations.r.get.nonEmpty) {connectionScore(row.sampleAnnotations.r.get, aSignature.r)}
          else {0.0}))
      scored
    }

  }

}
