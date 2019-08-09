package com.dataintuitive.luciuscore.analytical.io

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object BingIO {
  /**
    * Load best inferred genes
    */

  case class BINGrow(entrezID: String, geneSymbol: String, geneTitle: String, selfCorrelation: Double,
                     recall: Double, pvalue: Double, inferenceCategory: String)

  def readBING(spark: SparkSession, filepath: String):RDD[BINGrow] = {
    import spark.implicits._
    spark.read.option("header", "true").option("inferSchema", "true").csv(filepath)
      .as[BINGrow].rdd
  }

}
