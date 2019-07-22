package com.dataintuitive.luciuscore.io

import com.dataintuitive.luciuscore.GeneModel._
import ParseFunctions._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

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
    * @param notAvailable The character used to indicate an empty record in parsed file. Defaults to nothing, other
    *                     frequent possibility is
    * @return `Genes` datastructure (in-memory array of `GeneAnnotation`)
    */
    def loadGenesFromFile(sc: SparkContext,
               geneAnnotationsFile: String,
               delimiter: String = "\t", notAvailable: String = ""): Genes = {

//      require(features.length == 5, "The length of the features vector needs to 5")

      val featuresToExtract = Seq("probesetID", "ENTREZID", "ENSEMBL", "SYMBOL", "GENENAME")

      // gene annotation files are mbs maximum, no need to carry on using RDD after using textFile
      val rawGenes: Array[Array[String]] = sc.textFile(geneAnnotationsFile).map(_.split(delimiter)).collect

      val featureIndices = extractFeatureIndices(rawGenes, featuresToExtract)

      val splitGenesRdd: Array[Array[Option[String]]] =
        extractFeatures(rawGenes, featuresToExtract, includeHeader=false)


      // Turn into RDD containing objects
      val genes: Array[GeneAnnotation] =
        splitGenesRdd.map(row => new GeneAnnotation(
          probesetid = row(0),
          datatype = row(1),
          entrezid = row(2),
          ensemblid = row(3),
          symbol = row(4),
          name = row(5),
          family = row(6))
        )


      new Genes(genes)
  }

}