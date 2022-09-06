package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.model

import enumeratum.values._

import scala.collection.immutable

sealed abstract class MenuItemType(val value: Int, val name: String) extends IntEnumEntry with Serializable

// scalastyle:off magic.number
object MenuItemType extends IntEnum[MenuItemType] {

    case object Normal extends MenuItemType(0, "Normal")
    case object Pizza extends MenuItemType(1, "Pizza")
    case object Chicken extends MenuItemType(2, "Chicken")
    case object GiftCard extends MenuItemType(3, "GiftCard")
    case object Composite extends MenuItemType(4, "Composite")
    case object LoyaltyItem extends MenuItemType(5, "LoyaltyItem")
    case object Prepaid extends MenuItemType(6, "Prepaid")

    def values: immutable.IndexedSeq[MenuItemType] = findValues
}
