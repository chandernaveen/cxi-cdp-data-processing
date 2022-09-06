package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.model

import enumeratum.values._

import scala.collection.immutable

sealed abstract class ParbrinkRecordType(val value: String) extends StringEnumEntry with Serializable

object ParbrinkRecordType extends StringEnum[ParbrinkRecordType] {

    case object Categories extends ParbrinkRecordType("itemGroups")
    case object Customers extends ParbrinkRecordType("customers")
    case object HouseAccounts extends ParbrinkRecordType("houseAccounts")
    case object HouseAccountCharges extends ParbrinkRecordType("houseAccountsCharges")
    case object Locations extends ParbrinkRecordType("options")
    case object MenuItems extends ParbrinkRecordType("items")
    case object OrderTenders extends ParbrinkRecordType("tenders")
    case object Orders extends ParbrinkRecordType("orders")
    case object Destinations extends ParbrinkRecordType("destinations")

    def values: immutable.IndexedSeq[ParbrinkRecordType] = findValues
}
