package com.dataintuitive.luciuscore.analytical

import com.dataintuitive.luciuscore.Model.{INDEX, Index, NotationType, PROBESETID, RankVector, SYMBOL, SignatureType, Symbol}
import com.dataintuitive.luciuscore.analytical.GeneAnnotations.GeneAnnotationsDb
import com.dataintuitive.luciuscore.utilities.SignedString

object Signatures {

  /**
    * The base trait for a signature
    */
  abstract class SignatureV2 extends Serializable {

    val StringSignature: SignatureType
    val ordered: Boolean
    val r: RankVector

    // Notation can be one of Symbol, Probesetid, Index
    // The flow is Symbol -> Probesetid -> Index and back
    val notation: NotationType

    protected def createRanks: RankVector


    // for simplicity these functions handle no other cases, make sure to filter out 0 and such before using
    protected def signToInt(sign: String): Int = sign match {
      case "" => 1
      case "-" => -1
    }

    protected def intToSign(sign: Int): String = sign match {
      case 1 => ""
      case -1 => "-"
    }


    implicit def stringExtension(string: String) = new SignedString(string)

    override def toString = StringSignature.mkString(s"Signature of type ${notation}: [", ",", "]")

    val length = StringSignature.length
    val size = StringSignature.length

  }



  case class SymbolSignatureV2(signature: Array[Symbol], ordered: Boolean = true) extends SignatureV2 with Serializable {

    lazy val StringSignature: Array[String] = signature
    val notation: NotationType = SYMBOL
    val signDict = signature.map(x => (x.abs, x.sign))
    private val signs = signDict.map(_._2)
    private val intSigns = signs.map(signToInt(_))
    val values = signDict.map(_._1)

    def createRanks: RankVector = {
      if (ordered) {
        signature.zip(this.length to 1 by -1).zip(intSigns).map(x => x._1._2 * x._2 toDouble)
      } else {
        signature.zip(Array.fill(this.length)(1)).zip(intSigns).map(x => x._1._2 * x._2 toDouble)
      }
    }
    // Note: it is not typically valid to Zhang score symbol ranks against a database of probeset ranks
    // that only works if you have a 1:1 mapping between symbols and probesets, which is not true in the inferred L1000!
    val r: RankVector = createRanks

    /**
      * Gene Symbols may have multiple probesets - signs are distributed onto all associated probesets
      * Symbol -> Probeset - 1:n
      * Sign(Symbol) -> Sign(P1), Sign(P2)...
      */
    def translate2Probesetid(translator: GeneAnnotationsDb): ProbesetidSignatureV2 = {
      require(values.forall(symbol => translator.symbol2ProbesetidDict.contains(symbol)))
      val signedProbesets = values
        .map(symbol => translator.symbol2ProbesetidDict(symbol))
        .zip(signs).flatMap(x => x._1.map(y => x._2 + y))
      ProbesetidSignatureV2(signedProbesets)
    }


    /**
      * Gene symbols can have multiple indices
      * Symbol -> Index - 1:n
      * Sign(Symbol) -> Sign(i1), Sign(i2)...
      * @param translator
      * @return
      */
    def translate2Index(translator: GeneAnnotationsDb): IndexSignatureV2 = {
      require(values.forall(symbol => translator.symbol2ProbesetidDict.contains(symbol)))

      val probesets = values.map(symbol => translator.symbol2ProbesetidDict(symbol))
      val indices = probesets.map(probs4Symbol => probs4Symbol.map(translator.probesetid2IndexDict(_)))
      val signAndIndices = indices.zip(intSigns).map(_.swap)
      val signedIndices = signAndIndices.flatMap(x => x._2.map(y => x._1 * y))
      IndexSignatureV2(signedIndices)
    }

  }

  case class ProbesetidSignatureV2(signature: Array[Symbol], ordered: Boolean = true) extends SignatureV2 with Serializable {

    lazy val StringSignature: Array[String] = signature
    val notation: NotationType = PROBESETID
    val signDict = signature.map(x => (x.abs, x.sign))
    private val signs = signDict.map(_._2)
    private val intSigns = signs.map(signToInt(_))
    val values = signDict.map(_._1)

    def createRanks: RankVector = {
      if (ordered) {
        signature.zip(this.length to 1 by -1).zip(intSigns).map(x => x._1._2 * x._2 toDouble)
      } else {
        signature.zip(Array.fill(this.length)(1)).zip(intSigns).map(x => x._1._2 * x._2 toDouble)
      }
    }

    val r: RankVector = createRanks

    /**
      * Probesets for the same genes can potentially have differing signs. Usually we pick majority for the symbol.
      * In even conflicting situations, the signs annihilate, and thus the gene is removed from the signature.
      * Probeset -> Symbol - n:1
      * @param translator
      * @return
      */
    /**
    def translate2Symbol(translator: GeneAnnotationsDb): SymbolSignatureV2 = {
      require(values.forall(probeset => translator.probesetidVector.contains(probeset)))

      val translation = values.map(probeset => translator.probesetid2SymbolDict(probeset).get)
      val signedTranslation = translation.zip(intSigns)
      val newSigns = signedTranslation.groupBy(_._1)
        .map(x => (x._1, x._2.map(_._2).sum)).filter(_._2 != 0).map(x => (x._1, intToSign(x._2.signum)))

      val newSignedSymbolList = newSigns.map(x => x._2 + x._1).toArray
      SymbolSignatureV2(newSignedSymbolList)
    }**/


    /**
      * Probesets each have only one index, straightforward
      * Probeset -> Index - 1:1
      * @param translator
      * @return
      */
    def translate2Index(translator: GeneAnnotationsDb): IndexSignatureV2 = {
      require(values.forall(probeset => translator.probesetidVector.contains(probeset)))

      val translation = values.map(probeset => translator.probesetid2IndexDict(probeset))
      val signedTranslation = translation.zip(intSigns).map(x => x._2 * x._1)

      IndexSignatureV2(signedTranslation)
    }

  }

  case class IndexSignatureV2(signature: Array[Index], ordered: Boolean = true) extends SignatureV2 with Serializable {

    lazy val StringSignature: Array[String] = signature.map(_.toString)
    val notation: NotationType = INDEX
    val signDict = signature.map(x => (x.abs, x.signum))
    private val signs = signDict.map(_._2)
    private val intSigns = signs
    val values = signDict.map(_._1)

    def createRanks: RankVector = {
      if (ordered) {
        signature.zip(this.length to 1 by -1).zip(intSigns).map(x => x._1._2 * x._2 toDouble)
      } else {
        signature.zip(Array.fill(this.length)(1)).zip(intSigns).map(x => x._1._2 * x._2 toDouble)
      }
    }

    val r: RankVector = createRanks

    /**
      * Although a single symbol can have multiple indices, a single index is only mapped to one gene symbol.
      * With multiple indices that describe probesets for the same gene, we could have a need to collapse the indices.
      * Index -> Symbol - n:1
      * Sign(i) -> Sign(s)
      * @param translator
      * @return
      */
    /**
    def translate2Symbol(translator: GeneAnnotationsDb): SymbolSignatureV2 = {
      require(values.forall(index => translator.index2ProbesetidDict.keySet.contains(index)))

      val translationProbes = values.map(index => translator.index2ProbesetidDict(index))
      val translation = translationProbes.map(probeset => translator.probesetid2SymbolDict(probeset).get)
      val signedTranslation = translation.zip(signs)
      // need to perform sign annihilation if they cancel out
      val newSigns = signedTranslation.groupBy(_._1)
        .map(x => (x._1, x._2.map(_._2).sum)).filter(_._2 != 0).map(x => (x._1, intToSign(x._2.signum)))

      val newSignedSymbolList = newSigns.map(x => x._2 + x._1).toArray

      SymbolSignatureV2(newSignedSymbolList)
    }**/

    /**
      * Indices each have only one probeset
      * Index -> Probeset - 1:1
      * Sign(i) -> Sign(p)
      * @param translator
      * @return
      */
    def translate2Probeset(translator: GeneAnnotationsDb): ProbesetidSignatureV2 = {
      require(values.forall(index => translator.index2ProbesetidDict.keySet.contains(index)))

      val translation = values.map(index => translator.index2ProbesetidDict(index))
      val signedTranslation = translation.zip(intSigns).map(x => intToSign(x._2) + x._1)
      ProbesetidSignatureV2(signedTranslation)
    }


  }


}
