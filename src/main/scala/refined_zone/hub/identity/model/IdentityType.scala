package com.cxi.cdp.data_processing
package refined_zone.hub.identity.model

import enumeratum.values._

sealed abstract class IdentityType(val value: String) extends StringEnumEntry with Serializable {
    def code: String = value
}

object IdentityType extends StringEnum[IdentityType] {

    case object Email extends IdentityType("email")
    case object Phone extends IdentityType("phone")

    case object CombinationBin extends IdentityType("combination-bin")
    case object CombinationCard extends IdentityType("combination-card")

    case object IPv4 extends IdentityType("ipv4")
    case object IPv6 extends IdentityType("ipv6")

    case object MaidIDFA extends IdentityType("MAID-IDFA")
    case object MaidAAID extends IdentityType("MAID-AAID")
    case object MaidUnknown extends IdentityType("MAID-UNKNOWN")

    case object ThrotleId extends IdentityType("throtle-id")

    val values = findValues

}
