package com.dataintuitive.luciuscore

import com.dataintuitive.luciuscore.analytical.Bing.GeneType.GeneType
import com.dataintuitive.luciuscore.Model._

/**
  * A model for a gene annotation and a collection of genes.
  */
object GeneModel extends Serializable {

  /**
    * Class for holding information about a gene.
    */
  class GeneAnnotation(
                        val probesetid: Probesetid,
                        val entrezid: String,
                        val ensemblid: String,
                        val symbol: Symbol,
                        val name: String) extends Serializable {

    override def toString = s"${probesetid} (entrezid = ${entrezid}, ensemblid = ${ensemblid}, symbol = ${symbol}, name = ${name})"

  }


  /**
    * Convenience class for holding an `Array` of `Gene` with some values/methods to make life easier.
    *
    * @param genes An array of genes.
    */
  class Genes(val genes: Array[GeneAnnotation]) extends Serializable {

    /**
      * The input contains entries with multiple symbol names, separated by `///`.
      */
    private def splitGeneAnnotationSymbols(in: String, value: String): Array[(String, String)] = {
      val arrayString = in.split("///").map(_.trim)
      return arrayString.flatMap(name => Map(name -> value))
    }

    /**
      * Create a dictionary (`GeneDictionary`)
      */
    private def createGeneDictionary(genesRdd: Array[GeneAnnotation]): GeneDictionary =  {
      genesRdd
        .flatMap(ga => splitGeneAnnotationSymbols(ga.symbol, ga.probesetid))
        .toMap
    }

    /**
      * Dictionary to translate symbols to probsetids
      */
    val symbol2ProbesetidDict = createGeneDictionary(genes)

    /**
      * Dictionary to translate indices to probesetids.
      *
      * Remark: offset 1 is important for consistency when translating between dense and sparse format
      */
    val index2ProbesetidDict: Map[Int, Probesetid] =
    genes
      .map(_.probesetid)
      .zipWithIndex
      .map(tuple => (tuple._1, tuple._2 + 1))
      .map(_.swap)
      .toMap

    /**
      * A vector containing the probesetids representing the genes.
      */
    val probesetidVector = genes.map(_.probesetid)

  }

}
