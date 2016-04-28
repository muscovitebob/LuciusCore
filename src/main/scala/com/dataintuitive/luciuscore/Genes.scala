package com.dataintuitive.luciuscore

import com.dataintuitive.luciuscore.lowlevel.GeneDictionaryFunctions
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class Gene(
            val probesetid: Probesetid,
            val entrezid: String,
            val ensemblid: String,
            val symbol: Symbol,
            val name: String) extends Serializable {

  override def toString = s"${probesetid} (entrezid = ${entrezid}, ensemblid = ${ensemblid}, symbol = ${symbol}, name = ${name})"

}

class Genes(val genes: Array[Gene]) extends Serializable {

  // Dictionary to translate symbols to probsetids
  val symbol2ProbesetidDict = GeneDictionaryFunctions.createGeneDictionary(genes)

  // Dictionary to translate indices to probesetids
  // offset 1 is important for consistency when translating between dense and sparse format
  val index2ProbesetidDict: Map[Int, Probesetid] =
    genes
      .map(_.probesetid)
      .zipWithIndex
      .map(tuple => (tuple._1, tuple._2 + 1))
      .map(_.swap)
      .toMap

  val probesetidVector = genes.map(_.probesetid)

}

object Genes {

  def apply(sc: SparkContext,
            geneAnnotationsFile: String,
            delimiter: String = "\t"):Genes = {

    val rawGenesRdd = sc.textFile(geneAnnotationsFile)
    // The column headers are the first row split with the defined delimiter
    val geneFeatures = rawGenesRdd.first.split(delimiter).drop(1)
    // The rest of the data is handled similarly
    val splitGenesRdd = rawGenesRdd
      .zipWithIndex
      .filter(_._2 > 0) // drop first row
      .map(_._1) // index not needed anymore
      .map(_.split(delimiter))
    // Turn into RDD containing objects
    val genes: RDD[Gene] =
      splitGenesRdd.map(x => new Gene(x(0), x(1), x(2), x(3), x(4)))

    val asRdd = genes
    val asArray: Array[Gene] = genes.collect()

    new Genes(asArray)
  }
}