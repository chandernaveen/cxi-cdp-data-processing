package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.model

import refined_zone.hub.model.OrderTenderType

import enumeratum.values._

import scala.collection.immutable

sealed abstract class ParbrinkOrderTenderType(val value: Int, val name: String) extends IntEnumEntry with Serializable

// scalastyle:off magic.number
object ParbrinkOrderTenderType extends IntEnum[ParbrinkOrderTenderType] {

    final val ParbrinkToCxiTenderType: Map[Int, OrderTenderType] = Map(
        None.value -> OrderTenderType.Unknown,
        Cash.value -> OrderTenderType.Cash,
        CreditCard.value -> OrderTenderType.CreditCard,
        GiftCard.value -> OrderTenderType.GiftCard,
        GiftCertificate.value -> OrderTenderType.GiftCard,
        Check.value -> OrderTenderType.Other,
        HouseAccount.value -> OrderTenderType.Wallet,
        External.value -> OrderTenderType.Other,
        Debit.value -> OrderTenderType.CreditCard
    )

    case object None extends ParbrinkOrderTenderType(0, "None")
    case object Cash extends ParbrinkOrderTenderType(1, "Cash")
    case object CreditCard extends ParbrinkOrderTenderType(2, "CreditCard")
    case object GiftCard extends ParbrinkOrderTenderType(3, "GiftCard")
    case object GiftCertificate extends ParbrinkOrderTenderType(4, "GiftCertificate")
    case object Check extends ParbrinkOrderTenderType(5, "Check")
    case object HouseAccount extends ParbrinkOrderTenderType(6, "HouseAccount")
    case object External extends ParbrinkOrderTenderType(7, "External")
    case object Debit extends ParbrinkOrderTenderType(8, "Debit")

    def values: immutable.IndexedSeq[ParbrinkOrderTenderType] = findValues
}
