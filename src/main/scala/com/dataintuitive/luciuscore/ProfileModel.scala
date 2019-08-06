package com.dataintuitive.luciuscore

import com.dataintuitive.luciuscore.GeneModel.GenesV2
import com.dataintuitive.luciuscore.Model._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

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


  }
}
