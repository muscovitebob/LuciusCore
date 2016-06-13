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

  // Cache here, and make sure the rest uses the cached version
  val asRdd = rdd.cache

  // Using the Product's iterator, we can simulate the _1 method on Tuples
  // This can be a real bottleneck if not careful. Make it a function!
  def samplesRdd: RDD[String] = asRdd
    .map(_.productIterator
      .next()
      .toString
    )

  def samples = samplesRdd.collect

}

class Stats1D(val rdd: RDD[OneD], val features: Array[String]) extends Stats[OneD] with Serializable {

  def asRddKV = asRdd

}

class StatsT(val rdd: RDD[OneD], val features: Array[String]) extends Stats[OneD] with Serializable {

  def asRddKV = asRdd

  def addP(sc: SparkContext, StatsFile: String, delimiter: String = "\t", zeroValue: Double = 0.0) = {
    val statsP = Stats(sc, StatsFile, delimiter, zeroValue)
    val joinedRdd =
      asRdd
        .join(statsP.asRdd)
        .map { case (pwid, (t, p)) => (pwid, t, p) }
    new StatsTP(joinedRdd, features)
  }

  def addP(p:StatsT) = {
    val statsP = p
    val joinedRdd = asRdd.join(statsP.asRdd)
    new StatsTP(joinedRdd.map { case (pwid, (t, p)) => (pwid, t, p) }, features)
  }

}


class StatsTP(val rdd: RDD[TP], val features: Array[String]) extends Stats[TP] with Serializable {

  import TransformationFunctions._

  def asRddKV = asRdd.map { case (pwid, v1, v2) => (pwid, (v1, v2)) }

  def addRanks:StatsTPR = new StatsTPR (
    asRdd
      .map {
        v => (v._1, v._2, v._3, valueVector2AvgRankVectorWithZeros(v))
      },
    features
  )
}

class StatsTPR(val rdd: RDD[TPR], val features: Array[String]) extends Stats[TPR] with Serializable {

  def asRddKV = asRdd.map { case (pwid, v1, v2, v3) => (pwid, (v1, v2, v3)) }

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

  def asRddKV = asRdd.map { case (pwid, sample, compound, v1, v2, v3) => (pwid, (sample, compound, v1, v2, v3)) }
  def pwidLookup(s: String) = asRddKV.lookup(s)

  def addZhang(query:RankVector) = {
    val addedZhang = asRdd.map( x =>
      (x.pwid,
        x.sample,
        x.compound,
        x.t,
        x.p,
        x.r,
        connectionScore(x.r, query)
      ))
    new ZhangAnnotatedStatsTPR(addedZhang, features)
  }

}

class ZhangAnnotatedStatsTPR(val rdd:RDD[ZATPR], val features: Array[String]) extends Stats[ZATPR] with Serializable {

  def asRddKV = asRdd.map { case (pwid, sample, compound, v1, v2, v3, zhang) => (pwid, (sample, compound, v1, v2, v3, zhang)) }

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
        (x(0),
          x.drop(1).map { y => Try { y.toDouble }.toOption.getOrElse(zeroValue) }
          )
        }

    // Experiment: add caching already and make sure it's persisted
    // TODO -- Remove later when all runs smooth
    val statsRddCached = statsRdd.cache
    val cnt = statsRddCached.count

    new StatsT(statsRddCached, statsFeatures)
  }

  def apply(t: Stats1D, p: Stats1D): StatsTP = {
    val joinedRdd = t.asRdd.join(p.asRdd)
    new StatsTP(joinedRdd.map { case (pwid, (t, p)) => (pwid, t, p) }, t.features)
  }

}