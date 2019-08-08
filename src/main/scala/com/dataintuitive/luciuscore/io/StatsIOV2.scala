package com.dataintuitive.luciuscore.io

import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.mllib.linalg.distributed.{IndexedRowMatrix, BlockMatrix, IndexedRow}
import org.apache.spark.mllib.linalg.Vectors

object StatsIOV2 {
  /**
    * Load the t and p stats files. Make sure to keep track of the probesetIDs and the sampleIDs.
    * For transposition, implement conversions to MLLib block matrix and use the distributed transpose.
    */

  case class StatsData(sampleids: Vector[String], probesetids: Vector[String], dataMatrix: RDD[(Long, Vector[Double])]) {
    def transpose: StatsData = {
      val indexedRows = this.dataMatrix // dont create new indices, preserve older ones
        .zipWithIndex.map(_.swap).map(x => new IndexedRow(x._1, Vectors.dense(x._2._2.toArray)))
      val BlockMatrix = new IndexedRowMatrix(indexedRows).toBlockMatrix()
      val transposedRowMat = BlockMatrix.transpose.toIndexedRowMatrix
      val transposedRDD = transposedRowMat.rows.map(row => (row.index, row.vector.toArray.toVector))
      StatsData(this.probesetids, this.sampleids, transposedRDD)
    }
  }

  //def parseDouble(s)


  def loadFile(sc: SparkContext, filepath: String, separator: String): StatsData = {
    //val data = spark.read.option("sep", "\t").option("maxColumns", 35000).csv(filepath)
    // RDD ordering is the same as the tsv so long as we do not shuffle
    val raw = sc.textFile(filepath).map(_.split(separator).map(_.trim))
    val colnamesWithOffset = raw.take(1).head.toList
    val colnames = colnamesWithOffset match {
      case offset :: rest => rest
    }
    val rownamesWithOffset = raw.map(x => x(0)).collect.toList
    val rownames = rownamesWithOffset match {
      case offset :: rest => rest
    }

    val noColnames = raw.map(_.toList)
      .mapPartitionsWithIndex((index, el) => if (index == 0) el.drop(1) else el, preservesPartitioning = true)

    val numeric = noColnames.map(x => x match {
      case offset :: rest => rest
    })
      .map(x => x.map(_.toDouble)).map(_.toVector)
    StatsData(colnames.toVector, rownames.toVector, numeric.zipWithIndex().map(_.swap))
  }







}
