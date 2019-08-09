package com.dataintuitive.luciuscore.analytical

import com.dataintuitive.luciuscore.Model.{Probesetid, Symbol}
import com.dataintuitive.luciuscore.analytical.Bing.GeneType.GeneType

object GeneAnnotations {

  /**
    * Annotation format to process a fully inferred genome, created using the Subramanian Connectivity Map 2017 OLS model.
    * @param probesetid probeset ID from microarray (a row)
    * @param dataType is the probeset predicted? either a landmark gene, best inferred gene, or merely inferred
    * @param entrezid
    * @param ensemblid
    * @param symbol uniprot gene symbol
    * @param name full name
    * @param geneFamily gene family descriptor (e.g. Kinases)
    */
  class GeneAnnotationRecord(
                          val probesetid: Probesetid,
                          val dataType: GeneType,
                          val entrezid: Option[String],
                          val ensemblid: Option[String],
                          val symbol: Option[Symbol],
                          val name: Option[String],
                          val geneFamily: Option[String]) extends Serializable {

    def this(probesetid: Probesetid, dataType: GeneType,
             entrezid: String,
             ensemblid: String,
             symbol: String,
             name: String,
             geneFamily: String) {
      this(probesetid, dataType, Some(entrezid), Some(ensemblid), Some(symbol), Some(name),
        Some(geneFamily))
    }

    override def toString = s"ProbesetID: ${probesetid}: " +
      s"(entrezid = ${entrezid.getOrElse("NA")}, dataType = ${dataType}, ensemblid = ${ensemblid.getOrElse("NA")}," +
      s" symbol = ${symbol.getOrElse("NA")}," +
      s" name = ${name.getOrElse("NA")}, geneFamily=${geneFamily.getOrElse("NA")}"

  }

  class GeneAnnotationsDb(val genes: Array[GeneAnnotationRecord]) extends Serializable {

    // note: cannot be converted to RDD without introducing an index into GeneAnnotationV2. strictly ordered

    private def splitRecord(record: String): Array[String] = record.split("///").map(_.trim)

    private def splitAndAttach(maybeString: Option[String],
                               otherString: String): Array[(Option[String], String)] = maybeString match {
      case Some(name) => {
        val arrayString = splitRecord(name)
        arrayString.flatMap(name => Map(Some(name) -> otherString))
      }
      case None => Array((None -> otherString))
    }

    private def createGeneDictionary(genes: Array[GeneAnnotationRecord]): Map[Option[Symbol], Array[Probesetid]] = {
      genes.map(ga => (ga.symbol, ga.probesetid)).groupBy(_._1).map(x => (x._1, x._2.map(_._2)))
    }

    private def createInverseGeneDictionary(dict: Map[Option[Symbol], Array[Probesetid]]):
    Map[Probesetid, Option[Symbol]] = {
      dict.toList.map(_.swap).flatMap{ element =>
        if (element._1.length > 1) element._1.flatMap(probesetid => Array((probesetid, element._2)))
        else Array((element._1.head, element._2))
      }.toMap
    }

    val symbol2ProbesetidDict = createGeneDictionary(genes)

    val probesetid2SymbolDict = createInverseGeneDictionary(symbol2ProbesetidDict)


    // 1-based indexing
    val index2ProbesetidDict: Map[Int, Probesetid] =
      genes
        .map(_.probesetid)
        .zipWithIndex
        .map(tuple => (tuple._1, tuple._2 + 1))
        .map(_.swap)
        .toMap

    val probesetid2IndexDict = index2ProbesetidDict.map(_.swap)

    val probesetidVector = genes.map(_.probesetid)

    /**
      * Get a new gene annotations database after getting rid of undesirable gene symbols
      * (these could be ones where the t stats and p stats contain NAs, for example)
      * This allows you to keep the gene annotations up to date, which is important for indexing
      * when interacting with the profiles database (RDD[DbRow])
      */
    def removeBySymbol(geneSymbols: Set[String]): GeneAnnotationsDb = {
      val symbolToProbe = geneSymbols.flatMap(symbol => this.symbol2ProbesetidDict(Some(symbol)))
      new GeneAnnotationsDb(this.genes.filter(x => !symbolToProbe.contains(x.probesetid)))
    }

    def removeByProbeset(probesetIDs: Set[String]): GeneAnnotationsDb = {
      val probesetIDsInDatabase = this.genes.map(_.probesetid).toSet
      val relevantProbesets = probesetIDs.toSet intersect probesetIDsInDatabase
      new GeneAnnotationsDb(this.genes.filter(x => !relevantProbesets.contains(x.probesetid)))
    }



  }

}
