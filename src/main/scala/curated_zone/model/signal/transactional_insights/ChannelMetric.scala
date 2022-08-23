package com.cxi.cdp.data_processing
package curated_zone.model.signal.transactional_insights

import com.cxi.cdp.data_processing.curated_zone.model.signal.{Signal, SignalDomain}
import com.cxi.cdp.data_processing.refined_zone.hub.model.OrderChannelType

import enumeratum.values._

sealed abstract class ChannelMetric(
    val value: String,
    val orderChannelType: OrderChannelType
) extends StringEnumEntry
    with Signal
    with Product
    with Serializable {
    override def signalName: String = value
}

object ChannelMetric extends StringEnum[ChannelMetric] with SignalDomain[ChannelMetric] {

    override val signalDomainName = "channel_metrics"

    case object Unknown extends ChannelMetric("unknown_channel", OrderChannelType.Unknown)
    case object PhysicalLane extends ChannelMetric("physical_lane", OrderChannelType.PhysicalLane)
    case object PhysicalKiosk extends ChannelMetric("physical_kiosk", OrderChannelType.PhysicalKiosk)
    case object PhysicalPickup extends ChannelMetric("physical_pickup", OrderChannelType.PhysicalPickup)
    case object PhysicalDelivery extends ChannelMetric("physical_delivery", OrderChannelType.PhysicalDelivery)
    case object DigitalWeb extends ChannelMetric("digital_web", OrderChannelType.DigitalWeb)
    case object DigitalApp extends ChannelMetric("digital_app", OrderChannelType.DigitalApp)
    case object Other extends ChannelMetric("other_channel", OrderChannelType.Other)

    override val values = findValues
    override val signals: Seq[ChannelMetric] = values

    private final val channelTypeToChannelMetric = values.map(v => v.orderChannelType -> v).toMap

    def fromChannelType(channelType: OrderChannelType): ChannelMetric = channelTypeToChannelMetric(channelType)

    def fromChannelTypeCode(channelTypeCode: Int): Option[ChannelMetric] = {
        OrderChannelType.withValueOpt(channelTypeCode).map(fromChannelType)
    }

}
