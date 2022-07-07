package com.cxi.cdp.data_processing
package refined_zone.hub.model

import enumeratum.values.{IntEnum, IntEnumEntry}

sealed abstract class OrderStateType(val value: Int, val name: String) extends IntEnumEntry with CxiTaxonomy {
    def code: Int = value
}

// scalastyle:off magic.number
object OrderStateType extends IntEnum[OrderStateType] {

    case object Unknown extends OrderStateType(value = 0, name = "Unknown")
    case object Open extends OrderStateType(value = 1, name = "Open")
    case object Draft extends OrderStateType(value = 2, name = "Draft")
    case object Cancelled extends OrderStateType(value = 3, name = "Cancelled")
    case object Completed extends OrderStateType(value = 4, name = "Closed/Completed")
    case object Other extends OrderStateType(value = 99, name = "Other")

    val values = findValues

}
