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
                          val dataType: Option[GeneType],
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
      this(probesetid, Some(dataType), Some(entrezid), Some(ensemblid), Some(symbol), Some(name),
        Some(geneFamily))
    }

    override def toString = s"ProbesetID: ${probesetid}: " +
      s"(entrezid = ${entrezid.getOrElse("NA")}, dataType = ${dataType}, ensemblid = ${ensemblid.getOrElse("NA")}," +
      s" symbol = ${symbol.getOrElse("NA")}," +
      s" name = ${name.getOrElse("NA")}, geneFamily=${geneFamily.getOrElse("NA")}"

  }

  def splitRecord(record: String): Array[String] = record.split("///").map(_.trim)

  case class ProcessedRecord (val probesetid: Probesetid,
                              val dataType: Option[GeneType],
                              val entrezid: Option[Array[String]],
                              val ensemblid: Option[Array[String]],
                              val symbol: Option[Array[Symbol]],
                              val name: Option[Array[String]],
                              val geneFamily: Option[String])

  class GeneAnnotationsDb(val genes: Array[ProcessedRecord]) extends Serializable {

    def this(rawgenerecords: Array[GeneAnnotationRecord]) {
      this(rawgenerecords.map{x =>
        ProcessedRecord(
          x.probesetid,
          x.dataType,
          x.entrezid.map(splitRecord(_)),
          x.ensemblid.map(splitRecord(_)),
          x.symbol.map(splitRecord(_)),
          x.name.map(splitRecord(_)),
          x.geneFamily)}
        )
    }

    // note: cannot be converted to RDD without introducing an index into GeneAnnotationV2. strictly ordered


    /**
      * The input contains entries with multiple symbol names, separated by `///`.
      */
    private def splitGeneAnnotationSymbols(in: String, value: Array[String]): Array[(String, Array[String])] = {
      val arrayString = in.split("///").map(_.trim)
      return arrayString.flatMap(name => Map(name -> value))
    }

    private def splitAndAttach(maybeString: Option[String],
                               otherString: String): Array[(Option[String], String)] = maybeString match {
      case Some(name) => {
        val arrayString = splitRecord(name)
        arrayString.flatMap(name => Map(Some(name) -> otherString))
      }
      case None => Array((None -> otherString))
    }

    /**
      * Complications: gene annotations file is principally arranged in probesets. Each probeset has a unique ID.
      * However, a probeset may correspond to multiple gene symbols, and everything else, but symbols are most important
      * These are written in as "sym1 /// sym2". The task is then as follows: create a mapping from probeset to symbol list
      * reverse that and make it "sym1 -> [p1, p2,...]. Also sometimes symbols are absent with "---"
      * Absence needs to be handled in IO so it doesn't throw an exception in the dict
      * @return
      */
    private def createInverseGeneDictionary: Map[Probesetid, Option[Array[Symbol]]] = {
      genes.map(x => (x.probesetid, x.symbol)).toMap
    }

    private def createGeneDictionary: Map[Symbol, Array[Probesetid]] = {
      // always accessed with .get to avoid null exceptions on probesets with no symbol
      val initial = probesetid2SymbolDict.toList.filter(x => !x._2.isEmpty).map(x => (x._2.get, x._1))
      val next = initial.flatMap(x =>
        x._1.map(y =>
          (y, x._2))
      )
        val oneafter = next.groupBy(_._1)
      val after = oneafter.map(x => (x._1, x._2.map(_._2))).map(x => (x._1, x._2.toArray))
      after
    }


    val probesetid2SymbolDict = createInverseGeneDictionary

    val symbol2ProbesetidDict = createGeneDictionary

    val missingSymbolProbesets = probesetid2SymbolDict.filter(x => x._2 == None).keys


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
    /**
    def removeBySymbol(geneSymbols: Set[String]): GeneAnnotationsDb = {
      val symbolToProbe = geneSymbols.flatMap(symbol => this.symbol2ProbesetidDict.get(symbol)).flatten
      new GeneAnnotationsDb(this.genes.filter(x => !symbolToProbe.contains(x.probesetid)))
    }**/

    def removeByProbeset(probesetIDs: Set[String]): GeneAnnotationsDb = {
      val probesetIDsInDatabase = this.genes.map(_.probesetid).toSet
      val relevantProbesets = probesetIDs.toSet intersect probesetIDsInDatabase
      new GeneAnnotationsDb(this.genes.filter(x => !relevantProbesets.contains(x.probesetid)))
    }



  }

}
