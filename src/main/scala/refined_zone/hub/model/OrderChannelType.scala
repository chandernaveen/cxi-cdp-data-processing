package com.cxi.cdp.data_processing
package refined_zone.hub.model

import enumeratum.values._

sealed abstract class OrderChannelType(val value: Int, val name: String)
    extends IntEnumEntry
    with CxiTaxonomy
    with Serializable {
    def code: Int = value
}

// scalastyle:off magic.number
object OrderChannelType extends IntEnum[OrderChannelType] {

    case object Unknown extends OrderChannelType(0, name = "Unknown")
    case object PhysicalLane extends OrderChannelType(1, name = "Physical Lane")
    case object PhysicalKiosk extends OrderChannelType(2, name = "Physical Kiosk")
    case object PhysicalPickup extends OrderChannelType(3, name = "Physical Pickup")
    case object PhysicalDelivery extends OrderChannelType(4, name = "Physical Delivery")
    case object DigitalWeb extends OrderChannelType(5, name = "Digital Web")
    case object DigitalApp extends OrderChannelType(6, name = "Digital App")
    case object Other extends OrderChannelType(99, name = "Other")

    val values = findValues

}
