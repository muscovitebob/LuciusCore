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
  abstract class SignatureV2 {

    val StringSignature: SignatureType
    val ordered: Boolean
    //val r: RankVector

    // Notation can be one of Symbol, Probesetid, Index
    // The flow is Symbol -> Probesetid -> Index and back
    val notation: NotationType

    //protected def createRanks: RankVector

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
    private val values = signDict.map(_._1)

    private def createRanks: RankVector = {
      //if (ordered) {
        //signature.zip(this.length to 1 by -1).map{
          //case (symbol)
            //???
        //}
      //}
      ???
    }

    /**
      * Gene Symbols may have multiple probesets - signs are distributed onto all associated probesets
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
      * @param translator
      * @return
      */
    def translate2Index(translator: GenesV2): IndexSignatureV2 = {
      require(signature.forall(symbol => translator.symbol2ProbesetidDict.contains(Some(symbol))))
      val probesets = signature.flatMap(symbol => translator.symbol2ProbesetidDict(Some(symbol)))
      IndexSignatureV2(probesets.map(probeset => translator.probesetid2IndexDict(probeset)))
    }

  }

  case class ProbesetidSignatureV2(signature: Array[Symbol], ordered: Boolean = true) extends SignatureV2 with Serializable {

    lazy val StringSignature: Array[String] = signature
    val notation: NotationType = PROBESETID
    val signDict = signature.map(x => (x.abs, x.sign))
    private val signs = signDict.map(_._2)
    private val intSigns = signs.map(signToInt(_))
    private val values = signDict.map(_._1)

    /**
      * Probesets for the same genes can potentially have differing signs. Usually we pick majority for the symbol.
      * In even conflicting situations, the signs annihilate, and thus the gene is removed from the signature.
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
      * @param translator
      * @return
      */
    def translate2Index(translator: GenesV2): IndexSignatureV2 = {
      require(signature.forall(probeset => translator.probesetidVector.contains(probeset)))
      IndexSignatureV2(signature.map(probeset => translator.probesetid2IndexDict(probeset)))
    }

  }

  case class IndexSignatureV2(signature: Array[Index], ordered: Boolean = true) extends SignatureV2 with Serializable {

    lazy val StringSignature: Array[String] = signature.map(_.toString)
    val notation: NotationType = INDEX
    val signDict = signature.map(x => (x.abs, x.signum))
    private val signs = signDict.map(_._2)
    private val values = signDict.map(_._1)

    /**
      * A single index may correspond to multiple symbols. Then the final symbol is annihilated if the index sign cancels out
      * @param translator
      * @return
      */
    def translate2Symbol(translator: GenesV2): SymbolSignatureV2 = {
      require(values.forall(index => translator.index2ProbesetidDict.keySet.contains(index)))

      val translationProbes = values.map(index => translator.index2ProbesetidDict(index))
      val translation = translationProbes.map(probeset => translator.probesetid2SymbolDict(probeset).get)
      val signedTranslation = translation.zip(signs)
      val newSigns = signedTranslation.groupBy(_._1)
        .map(x => (x._1, x._2.map(_._2).sum.signum))

      val newSignedSymbolList = newSigns.filter{_._2 != 0}.map(_.swap).map(x => intToSign(x._1) + x._2).toArray

      SymbolSignatureV2(newSignedSymbolList)
    }

    /**
      * Indices each have only one probeset
      * @param translator
      * @return
      */
    def translate2Probeset(translator: GenesV2): ProbesetidSignatureV2 = {
      require(signature.forall(index => translator.index2ProbesetidDict.keySet.contains(index)))
      ProbesetidSignatureV2(signature.map(index => translator.index2ProbesetidDict(index)))
    }


  }



}