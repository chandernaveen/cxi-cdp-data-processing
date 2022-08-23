package com.cxi.cdp.data_processing
package refined_zone.hub.model

import enumeratum.values.{IntEnum, IntEnumEntry}

sealed abstract class LocationType(val value: Int, val name: String) extends IntEnumEntry with CxiTaxonomy {
    def code: Int = value
}

// scalastyle:off magic.number
object LocationType extends IntEnum[LocationType] {

    case object Unknown extends LocationType(value = 0, name = "Unknown")
    case object Restaurant extends LocationType(value = 1, name = "Restaurant")
    case object CStore extends LocationType(value = 2, name = "C-Store")
    case object Hotel extends LocationType(value = 3, name = "Hotel")
    case object Bar extends LocationType(value = 4, name = "Bar")
    case object Website extends LocationType(value = 5, name = "Website")
    case object Mobile extends LocationType(value = 6, name = "Mobile")
    case object Other extends LocationType(value = 99, name = "Other")

    val values = findValues

}
