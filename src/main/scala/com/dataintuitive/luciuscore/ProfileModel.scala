package com.dataintuitive.luciuscore

import com.dataintuitive.luciuscore.GeneModel.GenesV2
import com.dataintuitive.luciuscore.Model._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ProfileModel {
  /**
    * Object to deal with RDD[DbRow] AKA databases of transcriptomic profiles, against which connectivity scoring is possible
    */

  class ProfileDatabase(spark: SparkSession, database: RDD[DbRow], geneAnnotations: GenesV2) {
    // check consistency
    require(database.map(_.sampleAnnotations.t.get.length).filter(_ != geneAnnotations.genes.length).isEmpty)
    require(database.map(_.sampleAnnotations.p.get.length).filter(_ != geneAnnotations.genes.length).isEmpty)
    require(database.map(_.sampleAnnotations.r.get.length).filter(_ != geneAnnotations.genes.length).isEmpty)

    def dropGenes(droplist: Array[Symbol]): ProfileDatabase = ???

    def dropProbesets(droplist: Array[Probesetid]): ProfileDatabase = ???


  }
}
