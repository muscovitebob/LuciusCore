package com.dataintuitive.luciuscore

import com.dataintuitive.luciuscore.io.BingIO.BINGrow
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DataType

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

    val symbolToRow: Map[String, BINGrow] = createSymbolToRowDictionary
    val bingStatus: Map[String, GeneType.GeneType] = createBingStatusDictionary


  }


}
