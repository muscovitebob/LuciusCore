package com.dataintuitive.luciuscore

/**
  * Created by toni on 22/04/16.
  */
class GeneString(val string: String) {
  //  Take the sign of a gene in a signature
  def sign: String =
    if (string.startsWith ("-") ) "-"
    else ""

  //  Take the absolute value of a gene in a signature
  def abs: String =
    if (string.startsWith ("-") ) string.drop (1)
    else string
}
