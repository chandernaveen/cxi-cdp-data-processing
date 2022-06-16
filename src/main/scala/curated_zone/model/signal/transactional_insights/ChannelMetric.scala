package com.cxi.cdp.data_processing
package curated_zone.model.signal.transactional_insights

import com.cxi.cdp.data_processing.curated_zone.model.signal.{Signal, SignalDomain}
import com.cxi.cdp.data_processing.refined_zone.hub.model.ChannelType

import enumeratum.values._

sealed abstract class ChannelMetric(
    val value: String,
    val channelType: ChannelType
) extends StringEnumEntry
    with Signal
    with Serializable {
    override def signalName: String = value
}

object ChannelMetric extends StringEnum[ChannelMetric] with SignalDomain[ChannelMetric] {

    override val signalDomainName = "channel_metrics"

    case object Unknown extends ChannelMetric("unknown_channel", ChannelType.Unknown)
    case object PhysicalLane extends ChannelMetric("physical_lane", ChannelType.PhysicalLane)
    case object PhysicalKiosk extends ChannelMetric("physical_kiosk", ChannelType.PhysicalKiosk)
    case object PhysicalPickup extends ChannelMetric("physical_pickup", ChannelType.PhysicalPickup)
    case object PhysicalDelivery extends ChannelMetric("physical_delivery", ChannelType.PhysicalDelivery)
    case object DigitalWeb extends ChannelMetric("digital_web", ChannelType.DigitalWeb)
    case object DigitalApp extends ChannelMetric("digital_app", ChannelType.DigitalApp)
    case object Other extends ChannelMetric("other_channel", ChannelType.Other)

    override val values = findValues
    override val signals: Seq[ChannelMetric] = values

    private final val channelTypeToChannelMetric = values.map(v => v.channelType -> v).toMap

    def fromChannelType(channelType: ChannelType): ChannelMetric = channelTypeToChannelMetric(channelType)

    def fromChannelTypeCode(channelTypeCode: Int): Option[ChannelMetric] = {
        ChannelType.withValueOpt(channelTypeCode).map(fromChannelType)
    }

}
