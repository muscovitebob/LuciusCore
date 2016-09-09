package com.dataintuitive.luciuscore.utils

/**
  * A helper class to work with signed strings in gene lists and signatures.
  *
  * Can easily be used with implicits.
  */
class SignedString(val string: String) {
  /**
    * Return the sign (as `String`) of a `SignedString`.
    */
  def sign: String =
    if (string.startsWith ("-") ) "-"
    else ""

  /**
    * Return the absolute value (as `String`) of a `SignedString`.
    */
  def abs: String =
    if (string.startsWith ("-") ) string.drop (1)
    else string
}
