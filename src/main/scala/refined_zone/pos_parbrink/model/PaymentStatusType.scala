package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.model

import enumeratum.values._

import scala.collection.immutable

sealed abstract class PaymentStatusType(val value: String) extends StringEnumEntry with Serializable

object PaymentStatusType extends StringEnum[PaymentStatusType] {

    case object Deleted extends PaymentStatusType("deleted")
    case object Active extends PaymentStatusType("active")
    case object Inactive extends PaymentStatusType("inactive")

    def values: immutable.IndexedSeq[PaymentStatusType] = findValues
}
