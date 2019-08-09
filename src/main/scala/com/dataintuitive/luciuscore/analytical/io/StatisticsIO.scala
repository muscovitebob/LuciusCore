package com.dataintuitive.luciuscore.analytical.io

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.rdd.RDD

import scala.util.Try

object StatisticsIO {
  /**
    * Load the t and p stats files. Make sure to keep track of the probesetIDs and the sampleIDs.
    * For transposition, implement conversions to MLLib block matrix and use the distributed transpose.
    * Big remark: This turns out to be too slow in practice.
    */

  case class StatsData(columns: Vector[String], rows: Vector[String], dataMatrix: RDD[(Long, Vector[Double])]) {
    def transpose: StatsData = {
      val indexedRows = this.dataMatrix // dont create new indices, preserve older ones
        .zipWithIndex.map(_.swap).map(x => new IndexedRow(x._1, Vectors.dense(x._2._2.toArray)))
      val BlockMatrix = new IndexedRowMatrix(indexedRows).toBlockMatrix()
      val transposedRowMat = BlockMatrix.transpose.toIndexedRowMatrix
      val transposedRDD = transposedRowMat.rows.map(row => (row.index, row.vector.toArray.toVector))
      StatsData(this.rows, this.columns, transposedRDD)
    }
  }

  def isDouble(s: String): Boolean = Try(s.toDouble).isSuccess


  def loadFile(sc: SparkContext, filepath: String, separator: String,
               probesetsAsColumns: Boolean = true): (StatsData, List[String]) = {
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
    val numbersOnly = noColnames.map(x => x match {
      case offset :: rest => rest
    })

    if (!probesetsAsColumns) {

      // remove probesets that have immeasurable values, when they are columns

      val indexedEntries = numbersOnly.zipWithIndex.map(x => (x._1.zipWithIndex, x._2))
      val offendingColumnIndices = indexedEntries.map(x => (x._1.filter(y => !isDouble(y._1))
        .map(_._2), x._2)).filter(x => x._1.nonEmpty).flatMap(x => x._1).collect.toSet
      val filteredOutColumns = indexedEntries.map(x => (x._1.filter(y => !offendingColumnIndices(y._2)).map(x => x._1), x._2))
      val colnamesFiltered = colnames.zipWithIndex.filter(x => !offendingColumnIndices(x._2)).map(x => x._1)
      val colnamesOffending = colnames.zipWithIndex.filter(x => offendingColumnIndices(x._2)).map(x => x._1)

      val numeric = filteredOutColumns.map(x => (x._2, x._1.map(y => y.toDouble).toVector))

      (StatsData(colnamesFiltered.toVector, rownames.toVector, numeric), colnamesOffending)

    } else {
      // every row is a probeset - to get rid of NAs just remove the RDD elements

      val indexed = numbersOnly.zipWithIndex().map(_.swap)

      val whichNumericRows = indexed.map(x => (x._1, x._2.forall(isDouble(_)))).collect

      val badRowIndices = whichNumericRows.filter(x => x._2 == false).map(_._1)
      val goodRowIndices = whichNumericRows.filter(x => x._2 == true).map(_._1)

      val numericRows = indexed.filter(x => !badRowIndices.contains(x._1))
      val faultyRows = indexed.filter(x => !goodRowIndices.contains(x._1))

      val badRownames = rownames.zipWithIndex.filter(x => faultyRows.keys.collect.contains(x._2)).map(_._1)
      val goodRownames = rownames.zipWithIndex.filter(x => numericRows.keys.collect.contains(x._2)).map(_._1)



      val numeric = numericRows.map(x => (x._1, x._2.map(y => y.toDouble).toVector))

      (StatsData(colnames.toVector, goodRownames.toVector, numeric), badRownames)
    }
  }







}
