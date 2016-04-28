package com.dataintuitive.luciuscore

/**
  * Created by toni on 20/04/16.
  */
class Sample(val samplePwid:String,
             val  sampleBatch:String,
             val  samplePlateid:String,
             val  sampleWell:String,
             val  sampleProtocolName:String,
             val  sampleConcentration:String,
             val  sampleyear:String
                       ) extends Serializable {

  override def toString = s"${samplePwid}"

}
