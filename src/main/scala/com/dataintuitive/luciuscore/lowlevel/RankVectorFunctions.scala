package com.dataintuitive.luciuscore.lowlevel

import com.dataintuitive.luciuscore._

/**
  * Created by toni on 26/04/16.
  */
object RankVectorFunctions {

  implicit def stringExtension(string: String) = new GeneString(string)

  // Convert index-based signatures to rank vectors.
  // We have different signatures, for performance reasons and ease of use
  // Be careful, signature and vector indices are 1-based
  def signature2OrderedRankVector(s: IndexSignature, length: Int): RankVector = {
    val sLength = s.signature.length
    val ranks = (sLength to 1 by -1).map(_.toDouble)
    val unsignedRanks = s.signature zip ranks
    val signedRanks = unsignedRanks
      .map { case (signedIndex, unsignedRank) =>
        (signedIndex.abs.toInt, (signedIndex.sign + unsignedRank).toDouble)
      }.toMap
    val asSeq = for (el <- 1 to length by 1) yield signedRanks.getOrElse(el, 0.0)
    asSeq.toArray
  }

  // Convert index-based signatures to rank vectors.
  // We have different signatures, for performance reasons and ease of use
  // Be careful, signature and vector indices are 1-based
  def signature2UnorderedRankVector(s: IndexSignature, length: Int): RankVector = {
    val sLength = s.signature.length
    val ranks = (sLength to 1 by -1).map(_ => 1.0)
    val unsignedRanks = s.signature zip ranks
    val signedRanks = unsignedRanks
      .map { case (signedIndex, unsignedRank) =>
        (signedIndex.abs.toInt, (signedIndex.sign + unsignedRank).toDouble)
      }.toMap
    val asSeq = for (el <- 1 to length by 1) yield signedRanks.getOrElse(el, 0.0)
    asSeq.toArray
  }


  // This is for the underlying array of Strings representing indices
  // Be careful, signature and vector indices are 1-based
  def indexArray2OrderedRankVector(a: Array[String], length: Int): RankVector = {
    val sLength = a.length
    val ranks = (sLength to 1 by -1).map(_.toDouble)
    val unsignedRanks = a zip ranks
    val signedRanks = unsignedRanks
      .map { case (signedIndex, unsignedRank) =>
        (signedIndex.abs.toInt, (signedIndex.sign + unsignedRank).toDouble)
      }.toMap
    val asSeq = for (el <- 1 to length by 1) yield signedRanks.getOrElse(el, 0.0)
    asSeq.toArray
  }

  // Be careful: offset 1 for vectors for consistency!
  def nonZeroElements(v: RankVector, offset:Int = 1): Array[(Index, Rank)] = {
    v.zipWithIndex
      .map(x => (x._1, x._2 + offset))
      .map(_.swap)
      .filter(_._2 != 0.0)
  }

  // Convert rank vector to index signature
  // This is the poor man's approach, not taking into account duplicate entries and such.
  // Be careful, signature and vector indices are 1-based
  def rankVector2IndexSignature(v: RankVector): IndexSignature = {
    val nonzero = nonZeroElements(v)
    val asArrayInt = nonzero.map {
      case (unsignedIndex, signedRank) => ((signedRank.abs / signedRank) * unsignedIndex).toInt
    }
    val asArrayString = asArrayInt.map(_.toString)
    new IndexSignature(asArrayString)
  }

}
