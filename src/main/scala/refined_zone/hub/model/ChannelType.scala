package com.cxi.cdp.data_processing
package refined_zone.hub.model

import enumeratum.values._

sealed abstract class ChannelType(val value: Int) extends IntEnumEntry {
    def code: Int = value
}

// scalastyle:off magic.number
object ChannelType extends IntEnum[ChannelType] {

    case object Unknown extends ChannelType(0)
    case object PhysicalLane extends ChannelType(1)
    case object PhysicalKiosk extends ChannelType(2)
    case object PhysicalPickup extends ChannelType(3)
    case object PhysicalDelivery extends ChannelType(4)
    case object DigitalWeb extends ChannelType(5)
    case object DigitalApp extends ChannelType(6)
    case object Other extends ChannelType(99)

    val values = findValues

}
