package com.dataintuitive.luciuscore.io

import com.dataintuitive.luciuscore.{GeneAnnotation, Genes}
import com.dataintuitive.luciuscore.lowlevel.ParseFunctions._
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
    * @param features The feature names (column names in input file) to extract. Default is `Seq("probesetid", "entrezid", "ensemblid", "symbol", "name")`
    * @return `Genes` datastructure (in-memory array of `GeneAnnotation`)
    */
    def loadGenesFromFile(sc: SparkContext,
               geneAnnotationsFile: String,
               delimiter: String = "\t",
               features:Seq[String] = Seq("probesetid", "entrezid", "ensemblid", "symbol", "name")): Genes = {

      require(features.length == 5, "The length of the features vector needs to 5")

      val rawGenesRdd = sc.textFile(geneAnnotationsFile).map(_.split(delimiter))

      val splitGenesRdd = extractFeatures(rawGenesRdd, features, includeHeader=false)

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

}