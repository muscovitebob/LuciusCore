package com.dataintuitive.luciuscore.analytical

import com.dataintuitive.luciuscore.analytical.io.BingIO.BINGrow
import org.apache.spark.rdd.RDD

object Bing extends Serializable {

  object GeneType extends Enumeration {
    type GeneType = Value
    val Landmark, BING, Inferred = Value
  }

  /**
    * Extract useful information out of the best inferred gene annotation
    * @param bingrdd
    */

  class bingInformation(val bingrdd: RDD[BINGrow]) {

    val bing = bingrdd.collect

    private def createSymbolToRowDictionary: Map[String, BINGrow] = {
      bing.map(row => (row.geneSymbol -> row)).toMap
    }

    private def createBingStatusDictionary: Map[String, GeneType.GeneType] = {
      bing.map(row => (row.geneSymbol,
        if (row.inferenceCategory == "Best Inferred (BING)") GeneType.BING
        else if (row.inferenceCategory == "Inferred") GeneType.Inferred
        else GeneType.Inferred)).toMap
    }

    private def createBingStatusByEntrezDictionary: Map[String, GeneType.GeneType] = {
      bing.map(row => (row.entrezID,
        if (row.inferenceCategory == "Best Inferred (BING)") GeneType.BING
        else if (row.inferenceCategory == "Inferred") GeneType.Inferred
        else GeneType.Inferred)).toMap
    }

    val symbolToRow: Map[String, BINGrow] = createSymbolToRowDictionary
    val bingStatus: Map[String, GeneType.GeneType] = createBingStatusDictionary
    val bingStatusByEntrez: Map[String, GeneType.GeneType] = createBingStatusByEntrezDictionary


  }


}