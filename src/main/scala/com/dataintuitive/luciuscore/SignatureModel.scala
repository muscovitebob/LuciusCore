package com.dataintuitive.luciuscore

import com.dataintuitive.luciuscore.GeneModel.GenesV2
import com.dataintuitive.luciuscore.utilities.SignedString
import com.dataintuitive.luciuscore.Model._
import scala.math.{abs, signum}

/**
  * A signature is sparse representation of a vector referring to the indices in a dense array.
  *
  * 3 implementations exist, depending on the naming scheme involved:
  *
  *  1. `SymbolSignature` when using gene symbol notation (e.g. MELK, BRCA1, ...).
  *
  *  2. `ProbesetIdSignature` when using the probeset ids used in experiments.
  *
  *  3. `IndexSignature` when using indices referring to the dense vector/array.
  *
  * The natural order of things is: `Symbol -> Probesetid -> Index`
  */
object SignatureModel extends Serializable {

  /**
    * The base trait for a signature
    */
  trait Signature {

    type T
    type U

    val signature: SignatureType

    // Notation can be one of Symbol, Probesetid, Index
    // The flow is Symbol -> Probesetid -> Index and back
    val notation: NotationType

    implicit def stringExtension(string: String) = new SignedString(string)

    override def toString = signature.mkString(s"Signature of type ${notation}: [", ",", "]")

    val length = signature.length

  }

  /**
    * Companion object to the `Signature` trait.
    *
    * This object defines a convencience `apply` method to create a signature of any type:
    *
    * {{{
    * val symbolList = Signature(Array("symbol2", "-symbol1"), "symbol")
    * val probesetidList = Signature(Array("psid2", "-psid1"), "probesetid")
    * }}}
    *
    */
  object Signature {

    /**
      * This will be updated to reflect heuristics that distinguish the different types of
      * signatures. For now, we stick to SymbolSignature as the target type.
      */
    def apply(s: SignatureType): Signature = {
      SymbolSignature(s)
    }

    /**
      * Generate a Signature of provided type/notation.
      */
    def apply(s: SignatureType, notation: String): Signature = {
      notation match {
        case "symbol" => SymbolSignature(s)
        case "probesetid" => ProbesetidSignature(s)
        case "index" => IndexSignature(s)
        case _ => println("Wrong signature type, please try again"); SymbolSignature(s)
      }
    }
  }

  /**
    * Signature with symbol notation.
    */
  case class SymbolSignature(val signature: SignatureType) extends Signature with Serializable {

    val notation: NotationType = SYMBOL

    /**
      * Translate a Symbol signature to a Probesetid signature
      *
      * @param dict     A dictionary (`Map`) `Symbol -> Probesetid`
      * @param failover A placeholder for missing values
      */
    def translate2Probesetid(dict: Map[Symbol, Probesetid], failover: String = "OOPS"): ProbesetidSignature = {
      val safeTranslation = safeTranslate2Probesetid(dict)
      ProbesetidSignature(safeTranslation.map(_.getOrElse(failover)))
    }

    /**
      * Translate a Symbol signature to a Probesetid signature. The result is wrapped in an `Option`.
      *
      * @param dict A dictionary (`Map`) `Symbol -> Probesetid`
      */
    def safeTranslate2Probesetid(dict: Map[Symbol, Probesetid]): Array[Option[Probesetid]] = {
      signature.map { g =>
        val translation = dict.get(g.abs)
        translation.map(go => g.sign + go)
      }
    }

  }

  /**
    * Signature with probesetid notation.
    */
  case class ProbesetidSignature(val signature: SignatureType) extends Signature with Serializable {

    val notation: NotationType = PROBESETID

    def translate2Probesetid(inverseDict: Map[Symbol, Probesetid], failover: String = "OOPS") = this

    /**
      * Translate a Probeset signature _back_ to a Symbol signature.
      *
      * @param inverseDict A dictionary (`Map`) `Symbol -> Probesetid`
      * @param failover    A placeholder for missing values, default is "OOPS"
      */
    def translate2Symbol(inverseDict: Map[Symbol, Probesetid], failover: String = "OOPS"): SymbolSignature = {
      val safeTranslation = safeTranslate2Symbol(inverseDict)
      SymbolSignature(safeTranslation.map(_.getOrElse(failover)))
    }

    /**
      * Translate a Probesetid signature to a Index signature.
      *
      * @param dict     A dictionary (`Map`) `Index -> Probesetid`
      * @param failover A placeholder for missing values, default is "0"
      */
    def translate2Index(dict: Map[Index, Probesetid], failover: String = "0"): IndexSignature = {
      val safeTranslation = safeTranslate2Index(dict)
      IndexSignature(safeTranslation.map(_.getOrElse(failover)))
    }

    /**
      * Translate a Probesetid signature _back to a Symbol signature. The result is wrapped in an `Option`.
      *
      * @param inverseDict A dictionary (`Map`) `Symbol -> Probesetid`
      */
    def safeTranslate2Symbol(inverseDict: Map[Symbol, Probesetid]): Array[Option[Symbol]] = {
      val dict = inverseDict.map(_.swap)
      signature.map { g =>
        val translation = dict.get(g.abs)
        translation.map(go => g.sign + go)
      }
    }

    /**
      * Translate a Probesetid signature to a Index signature. The result is wrapped in an `Option`.
      *
      * @param dict A dictionary (`Map`) `Index -> Probesetid`
      */
    def safeTranslate2Index(dict: Map[Index, Probesetid]): Array[Option[String]] = {
      val inverseDict = dict.map(_.swap)
      signature.map { g =>
        val translation = inverseDict.get(g.abs)
        translation.map(go => g.sign + go)
      }
    }

  }

  /**
    * Signature with index notation.
    */
  case class IndexSignature(val signature: SignatureType) extends Signature with Serializable {

    val notation: NotationType = INDEX

    /**
      * Translate a Index signature _back to a Probesetid signature. The result is wrapped in an `Option`.
      *
      * @param inverseDict A dictionary (`Map`) `Symbol -> Probesetid`
      * @todo Check This out!!!
      */
    def safeTranslate2Symbol(inverseDict: Map[Symbol, Probesetid]): Array[Option[Symbol]] = {
      val dict = inverseDict.map(_.swap)
      signature.map { g =>
        val translation = dict.get(g.abs)
        translation.map(go => g.sign + go)
      }
    }

    /**
      * Translate a Index  signature to a Probesetid signature.
      *
      * @param dict     A dictionary (`Map`) `Index -> Probesetid`
      * @param failover A placeholder for missing values, default is "OOPS"
      */
    def translate2Probesetid(dict: Map[Index, Probesetid], failover: String = "OOPS") = {
      val safeTranslation = safeTranslate2Probesetid(dict)
      ProbesetidSignature(safeTranslation.map(_.getOrElse(failover)))
    }

    /**
      * Translate a Index signature _back to a Probesetid signature. The result is wrapped in an `Option`.
      *
      * @param dict A dictionary (`Map`) `Index -> Probesetid`
      */
    def safeTranslate2Probesetid(dict: Map[Index, Probesetid]): Array[Option[Probesetid]] = {
      signature.map { g =>
        val translation = dict.get(g.abs.toInt)
        translation.map(go => g.sign + go)
      }
    }

    // No bullet-proof approach, should provide failsafe here as well
    val asSignedInt = signature.map(_.toDouble.toInt)

  }

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
    def translate2Probesetid(translator: GenesV2): ProbesetidSignatureV2 = {
      require(values.forall(symbol => translator.symbol2ProbesetidDict.contains(Some(symbol))))
      val signedProbesets = values
        .map(symbol => translator.symbol2ProbesetidDict(Some(symbol)))
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
    def translate2Index(translator: GenesV2): IndexSignatureV2 = {
      require(values.forall(symbol => translator.symbol2ProbesetidDict.contains(Some(symbol))))

      val probesets = values.map(symbol => translator.symbol2ProbesetidDict(Some(symbol)))
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
    def translate2Symbol(translator: GenesV2): SymbolSignatureV2 = {
      require(values.forall(probeset => translator.probesetidVector.contains(probeset)))

      val translation = values.map(probeset => translator.probesetid2SymbolDict(probeset).get)
      val signedTranslation = translation.zip(intSigns)
      val newSigns = signedTranslation.groupBy(_._1)
        .map(x => (x._1, x._2.map(_._2).sum)).filter(_._2 != 0).map(x => (x._1, intToSign(x._2.signum)))

      val newSignedSymbolList = newSigns.map(x => x._2 + x._1).toArray
      SymbolSignatureV2(newSignedSymbolList)
    }


    /**
      * Probesets each have only one index, straightforward
      * Probeset -> Index - 1:1
      * @param translator
      * @return
      */
    def translate2Index(translator: GenesV2): IndexSignatureV2 = {
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
    def translate2Symbol(translator: GenesV2): SymbolSignatureV2 = {
      require(values.forall(index => translator.index2ProbesetidDict.keySet.contains(index)))

      val translationProbes = values.map(index => translator.index2ProbesetidDict(index))
      val translation = translationProbes.map(probeset => translator.probesetid2SymbolDict(probeset).get)
      val signedTranslation = translation.zip(signs)
      // need to perform sign annihilation if they cancel out
      val newSigns = signedTranslation.groupBy(_._1)
        .map(x => (x._1, x._2.map(_._2).sum)).filter(_._2 != 0).map(x => (x._1, intToSign(x._2.signum)))

      val newSignedSymbolList = newSigns.map(x => x._2 + x._1).toArray

      SymbolSignatureV2(newSignedSymbolList)
    }

    /**
      * Indices each have only one probeset
      * Index -> Probeset - 1:1
      * Sign(i) -> Sign(p)
      * @param translator
      * @return
      */
    def translate2Probeset(translator: GenesV2): ProbesetidSignatureV2 = {
      require(values.forall(index => translator.index2ProbesetidDict.keySet.contains(index)))

      val translation = values.map(index => translator.index2ProbesetidDict(index))
      val signedTranslation = translation.zip(intSigns).map(x => intToSign(x._2) + x._1)
      ProbesetidSignatureV2(signedTranslation)
    }


  }



}