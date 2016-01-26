package LuciusBack

import Math.{abs, max}

import GeneFunctions._
import CompoundFunctions._
import FeatureFunctions._
import RddFunctions._

object L1000 {

  // A vector containing t-values, or any other value for that matter
  type ValueVector = Array[Double]
  // A vector containing ranks.
  // Ranks can have fractional values, using Double for them
  type RankVector = Array[Double]

  // Create specific subtypes for t-stats and p-stats
  // But overkill
  type tStats = ValueVector
  type pStats = ValueVector

  // Signatures are used as queries (variable length)
  // When ordered, the first in the sequence gets the highest rank:
  type OrderedSignature = Array[Gene]
  // When unordered, they all get equal rank:
  type UnorderedSignature = Array[Gene]

  // mapper function for selection of features
  def featureSelection(x:AnyRef, features:List[String]) = {
    // Convert case class to Map(parameter -> value)
    // http://stackoverflow.com/a/1227643/4112468
    def getCCParams(cc: AnyRef) =
      (Map[String, Any]() /: cc.getClass.getDeclaredFields) {(a, f) =>
        f.setAccessible(true)
        a + (f.getName -> f.get(cc))
      }

    features.map(f => getCCParams(x).getOrElse(f, "NA")).toArray

  }

  // A simple approach to convert a value vector into a rank vector
  // This procedure does not take into account the ordering of equal values
  def valueVector2RankVector(vector: ValueVector): RankVector = {
    vector.zipWithIndex
      .sortBy(x => Math.abs(x._1))
      .zipWithIndex
      .sortBy(_._1._2)
      .map(x => if (x._1._1 >= 0) x._2 else -x._2)
      .map(_.toDouble)
  }

  // A better approach to converting a value vector into a rank vector.
  def valueVector2AvgRankVector(v: ValueVector): RankVector = {
    // Helper function
    def avg_unsigned_rank(ranks: RankVector): Double =
      ranks.foldLeft(0.)(+_ + _) / ranks.length

    v.zipWithIndex
      .sortBy(x => Math.abs(x._1))
      .zipWithIndex
      .sortBy(_._1._2)
      .map(x => Map[String, Double]("value" -> x._1._1, "orig_index" -> x._1._2, "unsigned_rank" -> x._2))
      .groupBy(x => Math.abs(x("value")))
      .map(_._2)
      .map(vector => vector.map(x => x ++ Map("avg_unsigned_rank" -> avg_unsigned_rank(vector.map(_("unsigned_rank"))))))
      .flatMap(x => x).toList
      .sortBy(_("orig_index"))
      .map(x => if (x("value") >= 0) x("avg_unsigned_rank") else -x("avg_unsigned_rank"))
      .toArray
  }

  // An implementation of this function that takes into account zero values.
  // A better approach to converting a value vector into a rank vector.
  // Be careful, ranks start at 1 in this implementation!
  def valueVector2AvgRankVectorWithZeros(v: ValueVector): RankVector = {
    // Helper function
    def avg_unsigned_rank(ranks: RankVector): Double = {
      ranks.foldLeft(0.)(+_ + _) / ranks.length
    }

    val zeros = v.filter(x => (x==0.)).length

    v.zipWithIndex
      .sortBy{ case(v,i) => Math.abs(v)}
      .zipWithIndex                         // add an index, this becomes the rank
      .sortBy{case ((v,i),j) => i}          // sort wrt original index
      .map{case ((v,i),j) => ((v,i),j+1)}   // make sure ranks start at 1 rather than 0
      .map{case ((v,i),j) => if (v==0.) ((v,i),0) else ((v,i),j-zeros)} // Make sure zero entries are not counted in rank
      .map{ case((v,i),j) => Map[String, Double]("value" -> v, "orig_index" -> i, "unsigned_rank" -> j)}
      .groupBy(x => Math.abs(x("value")))   // The specifics of the average rank calculation
      .map(_._2)
      .map(vector => vector.map(x => x ++ Map("avg_unsigned_rank" -> avg_unsigned_rank(vector.map(_("unsigned_rank"))))))
      .flatMap(x => x).toList
      .sortBy(_("orig_index"))
      .map(x => if (x("value") >= 0) x("avg_unsigned_rank") else -x("avg_unsigned_rank"))
      .toArray
  }


  // Convert an OrderedSignature to a full rank vector.
  def orderedSignature2RankVector(s: OrderedSignature, genes: Genes): RankVector = {
    val dict = genes.zipWithIndex.toMap

    val geneSignedRank = s.zip(s.length to 0 by -1)
      .map { case (gene, unsignedRank) =>
      (absGene(gene), signGene(gene) + unsignedRank)
    }
      .map { case (gene, signedRankStr) => (gene, signedRankStr.toDouble) }
      .map { case (gene, sRank) => (dict(gene), sRank) }.toMap

    List.tabulate(dict.size)(el => geneSignedRank.getOrElse(el, 0.))
      .toArray
  }

  // Convert a rank vector to an ordered signature
  // Take into account zeroes, aka remove them
  def rankVector2OrderedSignature(rankVector:RankVector, genes:Genes):OrderedSignature = {
    rankVector.
      zip(genes).
      filter{case (rank, gene) => rank != 0.0}.
      sortBy{case (rank, gene) => -Math.abs(rank)}.
      map{case (rank, gene) => if (rank < 0) "-" + gene else gene}
  }

  // Calculate the connection score/similarity between two rank vectors
  def connectionScore(ref_signed_ranks: RankVector, query_signed_ranks: RankVector): Double = {

    def connectionStrength(ref_signed_ranks: RankVector, query_signed_ranks: RankVector): Double =
      ref_signed_ranks.zip(query_signed_ranks).map { case (i, j) => i * j }.sum

    // This gives issues for all-zero vectors!
    def maxConnectionStrength(ref_signed_ranks: RankVector, query_signed_ranks: RankVector): Double = {
      val maxr = ref_signed_ranks.map(abs(_)).foldLeft(0.)(max(_, _))
      val maxq = query_signed_ranks.map(abs(_)).foldLeft(0.)(max(_, _))

      (maxr to (maxr - maxq) by -1).zip(maxq to 0 by -1).map { case (i, j) => i * j }.sum
    }

    connectionStrength(ref_signed_ranks, query_signed_ranks) /
      maxConnectionStrength(ref_signed_ranks, query_signed_ranks)
  }


  // Calculate median of a sequence of numbers
  def median(l:Seq[Double]): Double = {
    val lsorted = l.sorted
    val length = l.size
    if (length % 2 == 0) (lsorted.apply(length/2-1) + lsorted.apply(length/2))*1.0/2 else lsorted.apply(length/2)
  }

  // Calculate the intersection of a selection of compounds
  // And retrieve the median values of the significant t-stats
  // This is like the 'derived' expression vector for a set of compounds
  def medianIntersection(selection:Array[AnnotatedTPStats]):RankVector = {
    selection.
      flatMap{        // Join datasets and add index for genes
        x => x.p.zip(x.t).zipWithIndex.map{case ((p,t),i) => (i,(t, p))}
      }
      .groupBy{        // Group by gene
        case(i,(t,p)) => i
      }
      .map{            // Select significant t-values, else 0.
        case (i,a) => (i,a.map{case (j,(t,p)) => if (p<.05) t else 0.})
      }
      .map{            // Calculate median for the set of significant expressions
        case (i,a) => (i, if (a.min == 0.0) 0. else median(a) )
      }
      .toArray.sorted.map(_._2)    // Make sure the result is sorted again.
  }


  // Please note that the below is not compatible to the alternative derivation! -- TODO --
  def generateSignature(tpStats: Array[Tuple2[Double, Double]], genes: Genes, cutoff: Double, length: Int): OrderedSignature = {
    /**
    Definition of the ONE JNJ compound based signature:

    1. JNJ compound has no significant genes (i.e. all p values ³ 0.05): the signature will consist of two genes:

    gene 1: From the genes with a positive t sign; take that gene with the smalles p value
    gene2: From the genes with a negative t sign; take that gene with the smallest p value
    Rank both genes with respect to their t-value

    2. JNJ compound has one significant gene (p <0.05): the signature will consist of two genes:

    gene 1: the significant gene
    gene 2: From the genes that have the opposite sign compared to the significant gene: take that gene with the smalles p value
    Rank both genes: significant gene receives rank 2

    3. JNJ compound has two significant genes (p <0.05): the signature will consist of two genes

    gene1: the significant gene
    gene 2: the other significant gene
    if both have + or – t signs that is ok
    Rank both genes

    4. JNJ compound > 2 significant genes

    Take all significant genes up to 100 genes
    Rank all genes
      **/

    // take an array of tp-tuples and add
    def ordered_ranks_significance(vector: Array[Tuple2[Double, Double]], cutoff: Double, length: Int): Array[Tuple4[String, Int, Double, Double]] = {
      vector.zip(genes).
        map { case ((t, p), g) => (g, t, p) }.
        filter { case (g, t, p) => p < cutoff }. // cutoff
        sortBy { case (g, t, p) => -abs(t) }. // sort descending
        take(length). // Select top-length
        sortBy { case (g, t, p) => abs(t) }. // sort ascending
        zipWithIndex. // add rank
        map { case ((g, t, p), new_i) => // sign genes based on t-value
        if (t >= 0) (g, new_i + 1, t, p) else ("-" + g, new_i + 1, t, p) }.
        sortBy { case (g, r, t, p) => -r }
    }

    // Sorting triples based on the last one, begin significance
    implicit val pOrdering = Ordering.by((p: Tuple3[String, Double, Double]) => p._3)

    val significant_ranks = ordered_ranks_significance(tpStats, cutoff, length)
    val nrSignificant = tpStats.count{case (t,p) => p < cutoff}

    if (nrSignificant == 0) {
      val posTminP = tpStats.zip(genes).filter { case ((t, p), g) => t > 0 }.map { case ((t, p), g) => (g, t, p) }.min
      val negTminP = tpStats.zip(genes).filter { case ((t, p), g) => t < 0 }.map { case ((t, p), g) => (g, t, p) }.min
      return if (posTminP._3 <= negTminP._3)
        Array(posTminP._1, "-"+negTminP._1)
      else
        Array("-"+negTminP._1, posTminP._1)
    } else if (nrSignificant == 1) {
      val v1v = significant_ranks.map { case (g, rank, t, p) => g }
      val v1 = v1v(0)
      val otherTmin = {
        if (signGene(v1) == "")
          "-" + tpStats.zip(genes).filter { case ((t, p), g) => t < 0 }.map { case ((t, p), g) => (g, t, p) }.min._1
        else
          tpStats.zip(genes).filter { case ((t, p), g) => t > 0 }.map { case ((t, p), g) => (g, t, p) }.min._1
      }
      return Array(v1, otherTmin)
    } else {
      return significant_ranks.map { case (g, new_i, t, p) => g }
    }

  }


}

