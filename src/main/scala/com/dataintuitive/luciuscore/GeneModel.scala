package com.dataintuitive.luciuscore

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

  /**
    * Annotation format to process the new 22K gene format, with inferred genes.
    * @param probesetid
    * @param dataType
    * @param entrezid
    * @param ensemblid
    * @param symbol
    * @param name
    * @param geneFamily
    */
  class GeneAnnotationV2(
                          val probesetid: Probesetid,
                          val dataType: String,
                          val entrezid: Option[String],
                          val ensemblid: Option[String],
                          val symbol: Option[Symbol],
                          val name: Option[String],
                          val geneFamily: Option[String]) extends Serializable {

    def this(probesetid: Probesetid, dataType: String,
      entrezid: String,
      ensemblid: String,
      symbol: String,
      name: String,
      geneFamily: String) {
        this(probesetid, dataType, Some(entrezid), Some(ensemblid), Some(symbol), Some(name),
      Some(geneFamily))
    }

    override def toString = s"${probesetid} " +
      s"(entrezid = ${entrezid}, dataType = ${dataType}, ensemblid = ${ensemblid.getOrElse("NA")}, symbol = ${symbol.getOrElse("NA")}, " +
      s"name = ${name.getOrElse("NA")}, geneFamily=${geneFamily.getOrElse("NA")}"

  }

  class GenesV2(val genes: Array[GeneAnnotationV2]) extends Serializable {

    private def splitGeneAnnotationSymbols(symbol: Option[String],
                                           probesetid: String): Array[(Option[String], String)] = symbol match {
      case Some(name) => {
        val arrayString = name.split("///").map(_.trim)
        arrayString.flatMap(name => Map(Some(name) -> probesetid))
      }
      case None => Array((None -> probesetid))
    }

    private def createGeneDictionary(genes: Array[GeneAnnotationV2]): GeneDictionaryV2 = {
      genes.flatMap(ga => splitGeneAnnotationSymbols(ga.symbol, ga.probesetid))
        .groupBy(_._1).map(intermediate => intermediate._1 -> intermediate._2.map(_._2))
    }

    private def createInverseGeneDictionary(dict: GeneDictionaryV2): InverseGeneDictionaryV2 = {
      dict.toList.map(_.swap).flatMap{ element =>
        if (element._1.length > 1) element._1.flatMap(probesetid => Array((probesetid, element._2)))
        else Array((element._1.head, element._2))
      }.toMap
    }

    val symbol2ProbesetidDict = createGeneDictionary(genes)

    val probesetid2SymbolDict = createInverseGeneDictionary(symbol2ProbesetidDict)

    val index2ProbesetidDict: Map[Int, Probesetid] =
      genes
        .map(_.probesetid)
        .zipWithIndex
        .map(tuple => (tuple._1, tuple._2 + 1))
        .map(_.swap)
        .toMap

    val probesetidVector = genes.map(_.probesetid)



  }

}
