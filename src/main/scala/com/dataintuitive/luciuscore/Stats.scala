package com.dataintuitive.luciuscore

import com.dataintuitive.luciuscore._
import com.dataintuitive.luciuscore.lowlevel.{TransformationFunctions, VectorFunctions, ZhangScoreFunctions}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.Try

/**
  * Created by toni on 20/04/16.
  */

trait Stats[T <: Product] {

  val rdd: RDD[T]
  val features: Array[String]

  val asRdd = rdd

  // Using the Product's iterator, we can simulate the _1 method on Tuples
  val samplesRdd: RDD[String] = rdd
    .map(_.productIterator
      .next()
      .toString
    )

  val samples = samplesRdd.collect

}

class Stats1D(val rdd: RDD[OneD], val features: Array[String]) extends Stats[OneD] with Serializable {

  val asRddKV = rdd

}

class StatsT(val rdd: RDD[OneD], val features: Array[String]) extends Stats[OneD] with Serializable {

  val asRddKV = rdd

  def addP(sc: SparkContext, StatsFile: String, delimiter: String = "\t", zeroValue: Double = 0.0) = {
    val statsP = Stats(sc, StatsFile, delimiter, zeroValue)
    val joinedRdd = rdd.join(statsP.asRdd)
    new StatsTP(joinedRdd.map { case (pwid, (t, p)) => (pwid, t, p) }, features)
  }

  def addP(p:StatsT) = {
    val statsP = p
    val joinedRdd = rdd.join(statsP.asRdd)
    new StatsTP(joinedRdd.map { case (pwid, (t, p)) => (pwid, t, p) }, features)
  }

}


class StatsTP(val rdd: RDD[TP], val features: Array[String]) extends Stats[TP] with Serializable {

  import TransformationFunctions._

  val asRddKV = rdd.map { case (pwid, v1, v2) => (pwid, (v1, v2)) }

  def addRanks:StatsTPR = new StatsTPR (
    rdd
      .map {
        v => (v._1, v._2, v._3, valueVector2AvgRankVectorWithZeros(v))
      },
    features
  )
}

class StatsTPR(val rdd: RDD[TPR], val features: Array[String]) extends Stats[TPR] with Serializable {

  val asRddKV = rdd.map { case (pwid, v1, v2, v3) => (pwid, (v1, v2, v3)) }

  def addAnnotations(sampleCompoundRelations: SampleCompoundRelations):AnnotatedStatsTPR = {
    new AnnotatedStatsTPR(
      sampleCompoundRelations.asRddKV
      .join(this.asRddKV)
      .map {
        case (pwid, (relation, (t, p, r))) => (pwid, relation.sample, relation.compound, t, p, r)
      },
      features
    )
  }
}

class AnnotatedStatsTPR(val rdd:RDD[ATPR], val features: Array[String]) extends Stats[ATPR] with Serializable {

  import ZhangScoreFunctions._

  val asRddKV = rdd.map { case (pwid, sample, compound, v1, v2, v3) => (pwid, (sample, compound, v1, v2, v3)) }
  def pwidLookup(s: String) = asRddKV.lookup(s)

  def addZhang(query:RankVector) = {
    val addedZhang = rdd.map( x =>
      (x.pwid,
        x.sample,
        x.compound,
        x.t,
        x.t,
        x.r,
        connectionScore(x.r, query)
      ))
    new ZhangAnnotatedStatsTPR(addedZhang, features)
  }

}

class ZhangAnnotatedStatsTPR(val rdd:RDD[ZATPR], val features: Array[String]) extends Stats[ZATPR] with Serializable {

  val asRddKV = rdd.map { case (pwid, sample, compound, v1, v2, v3, zhang) => (pwid, (sample, compound, v1, v2, v3, zhang)) }

}


object Stats {

  /* A constructor that loads the data from a file
   */
  def apply(sc: SparkContext, StatsFile: String, delimiter: String = "\t", zeroValue: Double = 0.0) = {

    val rawStatsRdd = sc.textFile(StatsFile)
    // The rest of the data is handled similarly
    val splitStatsRdd = rawStatsRdd
      .map(_.split(delimiter))
    // Transpose the RDD
    val transposedStatsRdd = VectorFunctions.transpose(splitStatsRdd)
    // The column headers are the first row split with the defined delimiter
    val statsFeatures = transposedStatsRdd.first.drop(1)
    // Turn into RDD containing objects
    val statsRdd =
      transposedStatsRdd
        .zipWithIndex
        .filter(_._2 > 0) // drop first row
        .map(_._1) // index not needed anymore
        .map { x =>
        (x(0), x.drop(1).map { x => Try {
          x.toDouble
        }.toOption.getOrElse(0.0)
        })
      }

    new StatsT(statsRdd, statsFeatures)
  }

  def apply(t: Stats1D, p: Stats1D): StatsTP = {
    val joinedRdd = t.asRdd.join(p.asRdd)
    new StatsTP(joinedRdd.map { case (pwid, (t, p)) => (pwid, t, p) }, t.features)
  }

}