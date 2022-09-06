package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.model

import enumeratum.values.{IntEnum, IntEnumEntry}

import scala.collection.immutable

sealed abstract class CardBrandType(val value: Int, val name: String) extends IntEnumEntry with Serializable

// scalastyle:off magic.number
object CardBrandType extends IntEnum[CardBrandType] {

    case object None extends CardBrandType(0, "None")
    case object MasterCard extends CardBrandType(1, "MasterCard")
    case object Visa extends CardBrandType(2, "Visa")
    case object AmericanExpress extends CardBrandType(3, "American Express")
    case object DinersClub extends CardBrandType(4, "Diners Club")
    case object Discover extends CardBrandType(5, "Discover")
    case object EnRoute extends CardBrandType(6, "enRoute")
    case object JCB extends CardBrandType(7, "JCB")
    case object Private extends CardBrandType(8, "Private")

    def values: immutable.IndexedSeq[CardBrandType] = findValues

}
