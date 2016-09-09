package com.dataintuitive.luciuscore

/**
  * Created by toni on 27/04/16.
  */
object TransformationFunctions {

  /* We start with two simplistic approach to calculating a signature.
   * These approaches do not take into account significance or anything.
   */

  // Calculate the intersection of a selection of samples/compounds
  // by calculating the median values of the significant t-stats per features/gene.
  def valueVectorSelection2ValueVector(selection: Array[Tuple3[String, Array[Double], Array[Double]]], significanceThreshold: Double = 0.05): ValueVector = {
    selection.
      flatMap {
        // Join datasets and add index for genes
        x => {
          val t = x._1
          val p = x._2
          p.zip(t).zipWithIndex.map { case ((p, t), i) => (i, (t, p)) }
        }
      }
      .groupBy {
        // Group by gene
        case (i, (t, p)) => i
      }
      .map {
        // Select significant t-values, else 0.
        case (i, a) => (i, a.map { case (j, (t, p)) => if (p < significanceThreshold) t else 0.0 })
      }
      .map {
        // Calculate median for the set of significant expressions
        case (i, a) => (i, if (a.min == 0.0) 0.0 else median(a))
      }
      .toArray
      .sorted // Make sure the result is sorted again.
      .map(_._2)
  }

  // Calculate median of a sequence of numbers
  // This is just a helper function
  def median(l: Seq[Double]): Double = {
    val lsorted = l.sorted
    val length = l.size
    if (length % 2 == 0) (lsorted.apply(length / 2 - 1) + lsorted.apply(length / 2)) * 1.0 / 2 else lsorted.apply(length / 2)
  }



  // An implementation of this function that takes into account zero values.
  // A better approach to converting a value vector into a rank vector.
  // Be careful, ranks start at 1 in this implementation!
  def valueVector2AvgRankVectorWithZeros(tp: Tuple3[String, Array[Double], Array[Double]]): RankVector = {

    // Helper function
    def avgUnsignedRank(ranks: RankVector): Double = {
      ranks.foldLeft(0.0)(+_ + _) / ranks.length
    }

    val v = tp._1

    val zeros = v.count(_ == 0.0)

    v.zipWithIndex
      .sortBy {
        case (v, i) => Math.abs(v)
      }
      .zipWithIndex // add an index, this becomes the rank
      .sortBy { case ((v, i), j) => i } // sort wrt original index
      .map {
      // make sure ranks start at 1 rather than 0
      case ((v, i), j) => ((v, i), j + 1)
    }
      .map {
        // Make sure zero entries are not counted in rank
        case ((v, i), j) => if (v == 0.0) ((v, i), 0) else ((v, i), j - zeros)
      }
      .map {
        // For every feature: keep a map of the values relevant at this stage:
        // - The original value in the vector
        // - The original index in the vector
        // - The unsigned rank
        case ((v, i), j) => Map[String, Double]("value" -> v, "origIndex" -> i, "unsignedRank" -> j)
      }
      .groupBy {
        // The specifics of the average rank calculation:
        // If the absolute value is the same, we should calculate the average rank for these
        x => Math.abs(x("value"))
      }
      .values
      .map {
        // Add an annotation for the average unsigned rank
        vector => vector.map(x => x ++ Map("avgUnsignedRank" -> avgUnsignedRank(vector.map(_ ("unsignedRank")).toArray)))
      }
      .flatMap(x => x).toList
      .sortBy(_ ("origIndex")) // Recover the original ordering
      .map {
      // Add the sign to the and return only the ranks
      x => if (x("value") >= 0) x("avgUnsignedRank") else -x("avgUnsignedRank")
    }
      .toArray
  }


  /* The implementations below are left for reference, they are not correct !
   */

  // A simple approach to convert a value vector into a rank vector
  // This procedure does not take into account the ordering of equal values
  @deprecated
  def valueVector2RankVector(v: Tuple3[String, Array[Double], Array[Double]]): RankVector = {
    val t = v._1
    t.zipWithIndex
      .sortBy(x => Math.abs(x._1))
      .zipWithIndex
      .sortBy(_._1._2)
      .map(x => if (x._1._1 >= 0) x._2 else -x._2)
      .map(_.toDouble)
      .toArray
  }

  // A better approach to converting a value vector into a rank vector.
  // Still, significance is not taken into account
  @deprecated
  def valueVector2AvgRankVector(v: Tuple3[String, Array[Double], Array[Double]]): RankVector = {

    val t = v._1

    // Helper function
    def avg_unsigned_rank(ranks: RankVector): Double =
      ranks.foldLeft(0.0)(+_ + _) / ranks.length

    t.zipWithIndex
      .sortBy(x => Math.abs(x._1))
      .zipWithIndex
      .sortBy(_._1._2)
      .map(x => Map[String, Double]("value" -> x._1._1, "orig_index" -> x._1._2, "unsigned_rank" -> x._2))
      .groupBy(x => Math.abs(x("value")))
      .map(_._2)
      .map(vector => vector.map(x => x ++ Map("avg_unsigned_rank" -> avg_unsigned_rank(vector.map(_ ("unsigned_rank")).toArray))))
      .flatMap(x => x).toList
      .sortBy(_ ("orig_index"))
      .map(x => if (x("value") >= 0) x("avg_unsigned_rank") else -x("avg_unsigned_rank"))
      .toArray
  }


}
