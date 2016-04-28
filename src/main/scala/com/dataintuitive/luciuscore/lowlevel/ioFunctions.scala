package com.dataintuitive.luciuscore.lowlevel

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by toni on 28/04/16.
  */
object ioFunctions {

  def loadTsv(sc: SparkContext,
                                  sampleCompoundAnnotationsFile: String,
                                  delimiter: String = "\t"):RDD[Array[String]] = {

    val rawSampleCompoundAnnotationsRdd = sc.textFile(sampleCompoundAnnotationsFile)
    // The column headers are the first row split with the defined delimiter
    val sampleCompoundFeatures = rawSampleCompoundAnnotationsRdd.first.split(delimiter).drop(1)
    // The rest of the data is handled similarly
    rawSampleCompoundAnnotationsRdd
      .zipWithIndex
      .filter(_._2 > 0) // drop first row
      .map(_._1) // index not needed anymore
      .map(_.split(delimiter))

  }

}
