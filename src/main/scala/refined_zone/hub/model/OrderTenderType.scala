package com.cxi.cdp.data_processing
package refined_zone.hub.model

import enumeratum.values.{IntEnum, IntEnumEntry}

sealed abstract class OrderTenderType(val value: Int, val name: String) extends IntEnumEntry with CxiTaxonomy {
    def code: Int = value
}

// scalastyle:off magic.number
object OrderTenderType extends IntEnum[OrderTenderType] {

    case object Unknown extends OrderTenderType(value = 0, name = "Unknown")
    case object Cash extends OrderTenderType(value = 1, name = "Cash")
    case object CreditCard extends OrderTenderType(value = 2, name = "Credit Card")
    case object GiftCard extends OrderTenderType(value = 3, name = "Gift Card")
    case object Wallet extends OrderTenderType(value = 4, name = "Wallet")
    case object Other extends OrderTenderType(value = 99, name = "Other")

    val values = findValues

}
