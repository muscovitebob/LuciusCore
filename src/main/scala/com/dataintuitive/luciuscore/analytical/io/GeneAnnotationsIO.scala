package com.dataintuitive.luciuscore.analytical.io

import com.dataintuitive.luciuscore.analytical.GeneAnnotations._
import com.dataintuitive.luciuscore.analytical.Bing.{GeneType, bingInformation}
import com.dataintuitive.luciuscore.analytical.io.BingIO.readBING
import com.dataintuitive.luciuscore.io.ParseFunctions.extractFeatures
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object GeneAnnotationsIO {

  def loadGeneAnnotationsFromFile(spark: SparkSession,
                          geneAnnotationsFile: String,
                          delimiter: String = "\t", bingAnnotationsFile: String): GeneAnnotationsDb  = {

    val featuresToExtract = Seq("probesetID", "dataType", "ENTREZID", "ENSEMBL", "SYMBOL", "GENENAME")
    val rawGenesRdd = spark.sparkContext.textFile(geneAnnotationsFile).map(_.split(delimiter))
    val splitGenesRdd = extractFeatures(rawGenesRdd, featuresToExtract, includeHeader=false)

    val genesRddFormatted = splitGenesRdd.map(row => row.map{element =>
      if (element.isEmpty) None
      else if (isNotProvided(element)) None
      else Some(element.get)
    })

    val bingRDD = readBING(spark, bingAnnotationsFile)
    val geneTypeDict = new bingInformation(bingRDD).bingStatus.map(x => (x._1, Some(x._2)))

    val genes: RDD[GeneAnnotationRecord] = genesRddFormatted.map(row =>
      new GeneAnnotationRecord(row(0).get,

        row(1).get match {
        case "LM" => Some(GeneType.Landmark)
        case "INF" => geneTypeDict.getOrElse(row(4).get, None)
        case _ => None
      }
        ,
        row(2), row(3), row(4), row(5), None))

    new GeneAnnotationsDb(genes.collect)
  }

  def isNotProvided(element: Option[String]): Boolean = {
    element.get == "---"
  }

}
