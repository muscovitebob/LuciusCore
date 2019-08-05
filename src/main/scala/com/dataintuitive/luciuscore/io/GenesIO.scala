package com.dataintuitive.luciuscore.io

import com.dataintuitive.luciuscore.GeneModel._
import ParseFunctions._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import com.dataintuitive.luciuscore.io.BingIO._
import com.dataintuitive.luciuscore.Bing._

/**
  * IO convenience functions, loading from file using Spark functionality where appropriate.
  */
object GenesIO {

  /**
    * IO convenience and demonstration function. Reading gene annotations from a file and parsing it
    * to a `Genes` datastructure.
    *
    * Please note that missing values are converted to `NA`.
    *
    * The default value of the features to extract are for a file of the following format (tab-separated):
    *
    * {{{
    * probesetid  entrezid  ensemblid symbol  name
    * psid1 entrezid1 ens1  symbol1 name1
    * psid2 entrezid2 ens2  symbol2 name2
    * psid3 entrezid3 ens3  symbol3 name3
    * }}}
    *
    * Remark: Even if the input does not contain all info to fill the datastructure, we need to provide a features vector of size 5!
    *
    * @param sc SparkContext
    * @param geneAnnotationsFile The location of the file
    * @param delimiter The delimiter to use when parsing the input file. Default is `tab`
    * @return `Genes` datastructure (in-memory array of `GeneAnnotation`)
    */
    def loadGenesFromFile(sc: SparkContext,
               geneAnnotationsFile: String,
               delimiter: String = "\t"): Genes = {

//      require(features.length == 5, "The length of the features vector needs to 5")

      val featuresToExtract = Seq("probesetID", "ENTREZID", "ENSEMBL", "SYMBOL", "GENENAME")

      val rawGenesRdd = sc.textFile(geneAnnotationsFile).map(_.split(delimiter))

      val splitGenesRdd = extractFeatures(rawGenesRdd, featuresToExtract, includeHeader=false)

      // Turn into RDD containing objects
      val genes: RDD[GeneAnnotation] =
        splitGenesRdd.map(x => new GeneAnnotation(
          x(0).getOrElse("NA"),
          x(1).getOrElse("NA"),
          x(2).getOrElse("NA"),
          x(3).getOrElse("NA"),
          x(4).getOrElse("NA"))
        )

      val asArray: Array[GeneAnnotation] = genes.collect()

      new Genes(asArray)
    }

    def loadGenesFromFileV2(spark: SparkSession,
                            geneAnnotationsFile: String,
                            delimiter: String = "\t"): GenesV2 = {

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

      val genes: RDD[GeneAnnotationV2] = genesRddFormatted.map(row =>
        new GeneAnnotationV2(row(0).get, row(1).get match {
          case "LM" => GeneType.Landmark
          case "Inf" => geneTypeDict(row(1).get)
        },
        row(2), row(3), row(4), row(5), row(6)))

      new GenesV2(genes.collect)
    }

    def isNotProvided(element: Option[String]): Boolean = {
      element.get == "---"
    }

}