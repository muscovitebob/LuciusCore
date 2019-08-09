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

    val featuresToExtract = Seq("probesetID", "dataType", "ENTREZID", "ENSEMBL", "SYMBOL", "GENENAME", "GENEFAMILY")
    val rawGenesRdd = spark.sparkContext.textFile(geneAnnotationsFile).map(_.split(delimiter))
    val splitGenesRdd = extractFeatures(rawGenesRdd, featuresToExtract, includeHeader=false)

    val genesRddFormatted = splitGenesRdd.map(row => row.map{element =>
      if (element.isEmpty) None
      else if (isNotProvided(element)) None
      else Some(element.get)
    })

    val bingRDD = readBING(spark, "src/main/resources/BING.csv")
    val geneTypeDict = new bingInformation(bingRDD).bingStatus

    val genes: RDD[GeneAnnotationRecord] = genesRddFormatted.map(row =>
      new GeneAnnotationRecord(row(0).get, row(1).get match {
        case "LM" => GeneType.Landmark
        case "Inf" => geneTypeDict(row(1).get)
      },
        row(2), row(3), row(4), row(5), row(6)))

    new GeneAnnotationsDb(genes.collect)
  }

  def isNotProvided(element: Option[String]): Boolean = {
    element.get == "---"
  }

}
