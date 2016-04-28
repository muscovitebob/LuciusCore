package com.dataintuitive.luciuscore

/*
 * A signature is sparse representation of a vector referring to the indices in the dense array
 */

trait Signature {

  type T
  type U

  val signature: SignatureType

  // Notation can be one of Symbol, Probesetid, Index
  // The flow is Symbol -> Probesetid -> Index and back
  val notation: NotationType

  implicit def stringExtension(string: String) = new GeneString(string)

  override def toString = signature.mkString(s"Signature of type ${notation}: [", ",", "]")

  val length = signature.length

}

object Signature {

  /* This will be updated to reflect heuristics that distinguish the different types of
   * signatures. For now, we stick to SymbolSignature as the target type.
   */
  def apply(s: SignatureType): Signature = {
    new SymbolSignature(s)
  }

  /* Generate Signature of provided type/notation.
   *   ! This does not work as expected, the class is registered as Signature which does not have the correct methods
   *   -- TODO --
   */
  def apply(s: SignatureType, notation: String) = {
    notation match {
      case "symbol" => new SymbolSignature(s)
      case "probesetid" => new ProbesetidSignature(s)
      case "index" => new IndexSignature(s)
      case _ => println("Wrong signature type, please try again") ; SymbolSignature(s)
    }
  }

}

case class SymbolSignature(val signature: SignatureType) extends Signature with Serializable {

  val notation: NotationType = SYMBOL

  def translate2Probesetid(dict: Map[Symbol, Probesetid], failover: String = "OOPS"): ProbesetidSignature = {
    val safeTranslation = safeTranslate2Probesetid(dict)
    new ProbesetidSignature(safeTranslation.map(_.getOrElse(failover)))
  }

  // Returns an Array of Option[String] for exception handling
  def safeTranslate2Probesetid(dict: Map[Symbol, Probesetid]): Array[Option[Probesetid]] = {
    signature.map { g =>
      val translation = dict.get(g.abs)
      translation.map(go => g.sign + go)
    }
  }

}

class ProbesetidSignature(val signature: SignatureType) extends Signature with Serializable {

  val notation: NotationType = PROBESETID

  def translate2Probesetid(inverseDict: Map[Symbol, Probesetid], failover: String = "OOPS") = this

  def translate2Symbol(inverseDict: Map[Symbol, Probesetid], failover: String = "OOPS"): SymbolSignature = {
    val safeTranslation = safeTranslate2Symbol(inverseDict)
    new SymbolSignature(safeTranslation.map(_.getOrElse(failover)))
  }

  def translate2Index(dict: Map[Index, Probesetid], failover: String = "0"): IndexSignature = {
    val safeTranslation = safeTranslate2Index(dict)
    new IndexSignature(safeTranslation.map(_.getOrElse(failover)))
  }

  // Returns an Array of Option[String] for exception handling
  def safeTranslate2Symbol(inverseDict: Map[Symbol, Probesetid]): Array[Option[Symbol]] = {
    val dict = inverseDict.map(_.swap)
    signature.map { g =>
      val translation = dict.get(g.abs)
      translation.map(go => g.sign + go)
    }
  }

  // Returns an Array of Option[String] for exception handling
  // We need the inverse dict, in order to have unique probsetids
  def safeTranslate2Index(dict: Map[Index, Probesetid]): Array[Option[String]] = {
    val inverseDict = dict.map(_.swap)
    signature.map { g =>
      val translation = inverseDict.get(g.abs)
      translation.map(go => g.sign + go)
    }
  }

}

class IndexSignature(val signature: SignatureType) extends Signature with Serializable {

  val notation: NotationType = INDEX

  def translate2Probesetid(dict: Map[Index, Probesetid], failover: String = "OOPS") = {
    val safeTranslation = safeTranslate2Probesetid(dict)
    new ProbesetidSignature(safeTranslation.map(_.getOrElse(failover)))
  }

  // Returns an Array of Option[String] for exception handling
  def safeTranslate2Probesetid(dict: Map[Index, Probesetid]): Array[Option[Probesetid]] = {
    signature.map { g =>
      val translation = dict.get(g.abs.toInt)
      translation.map(go => g.sign + go)
    }
  }

  // No bullet-proof approach, should provide failsafe here as well
  val asSignedInt = signature.map(_.toDouble.toInt)

}
