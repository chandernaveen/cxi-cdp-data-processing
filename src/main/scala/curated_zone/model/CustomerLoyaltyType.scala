package com.cxi.cdp.data_processing
package curated_zone.model

import enumeratum.values._

import scala.collection.immutable

sealed abstract class CustomerLoyaltyType(val value: String) extends StringEnumEntry with Serializable

object CustomerLoyaltyType extends StringEnum[CustomerLoyaltyType] {

    case object AtRisk extends CustomerLoyaltyType("AT_RISK")
    case object Loyal extends CustomerLoyaltyType("LOYAL")
    case object New extends CustomerLoyaltyType("NEW")
    case object Regular extends CustomerLoyaltyType("REGULAR")

    val values: immutable.IndexedSeq[CustomerLoyaltyType] = findValues

}
