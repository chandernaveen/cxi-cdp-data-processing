package com.cxi.cdp.data_processing
package raw_zone.pos_square.model

case class OrderSource(name: String)

object OrderSource {

    object SourceNamePrefix {
        val Popmenu = "Popmenu"
        val Doordash = "Doordash"
        val Ubereats = "Ubereats"
        val Grubhubweb = "Grubhubweb"
        val SquareOnline = "Square Online"
    }

}
