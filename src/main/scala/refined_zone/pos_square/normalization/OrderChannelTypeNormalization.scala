package com.cxi.cdp.data_processing
package refined_zone.pos_square.normalization

import raw_zone.pos_square.model.{Fulfillment, OrderSource}
import refined_zone.hub.model.OrderChannelType
import support.normalization.CxiTaxonomyNormalization

case object OrderChannelTypeNormalization extends CxiTaxonomyNormalization {

    private final val DigitalWebOrderSourceNamePrefixes = Seq(
        OrderSource.SourceNamePrefix.Popmenu,
        OrderSource.SourceNamePrefix.SquareOnline
    )

    private final val DigitalAppOrderSourceNamePrefixes = Seq(
        OrderSource.SourceNamePrefix.Doordash,
        OrderSource.SourceNamePrefix.Ubereats,
        OrderSource.SourceNamePrefix.Grubhubweb
    )

    def normalizeOrderOriginateChannelType(maybeOrderSource: Option[OrderSource]): OrderChannelType = {
        val maybeOrderSourceName = maybeOrderSource.flatMap(orderSource => Option(orderSource.name))

        maybeOrderSourceName
            .flatMap { orderSourceName =>
                if (DigitalWebOrderSourceNamePrefixes.exists(prefix => orderSourceName.startsWith(prefix))) {
                    Some(OrderChannelType.DigitalWeb)
                } else if (DigitalAppOrderSourceNamePrefixes.exists(prefix => orderSourceName.startsWith(prefix))) {
                    Some(OrderChannelType.DigitalApp)
                } else {
                    None
                }
            }
            .getOrElse(OrderChannelType.PhysicalLane) // fallback
    }

    /** Determines order target channel type from several sources in the following order:
      * - order source
      * - fulfillments
      * - fallback to Unknown
      */
    def normalizeOrderTargetChannelType(
        maybeOrderSource: Option[OrderSource],
        maybeFulfillments: Option[Seq[Fulfillment]]
    ): OrderChannelType = {

        maybeOrderSource
            .flatMap(orderTargetChannelTypeFromOrderSource _)
            .orElse(maybeFulfillments.flatMap(orderTargetChannelTypeFromFulfillments _))
            .getOrElse(OrderChannelType.Unknown) // fallback
    }

    private final val PhysicalDeliveryOrderSourceNamePrefixes = Seq(
        OrderSource.SourceNamePrefix.Popmenu,
        OrderSource.SourceNamePrefix.Doordash,
        OrderSource.SourceNamePrefix.Ubereats,
        OrderSource.SourceNamePrefix.Grubhubweb,
        OrderSource.SourceNamePrefix.SquareOnline
    )

    private[normalization] def orderTargetChannelTypeFromOrderSource(
        orderSource: OrderSource
    ): Option[OrderChannelType] = {
        Option(orderSource.name).flatMap { orderSourceName =>
            if (PhysicalDeliveryOrderSourceNamePrefixes.exists(prefix => orderSourceName.startsWith(prefix))) {
                Some(OrderChannelType.PhysicalDelivery)
            } else {
                None
            }
        }
    }

    private[normalization] def orderTargetChannelTypeFromFulfillments(
        fulfillments: Seq[Fulfillment]
    ): Option[OrderChannelType] = {
        val (completedFulfillments, otherFulfillments) =
            fulfillments.partition(_.state == Fulfillment.State.Completed)

        // prefer (non-null) type from COMPLETED fulfillments
        val fulfillmentType = (completedFulfillments ++ otherFulfillments)
            .flatMap(fulfillment => Option(fulfillment.`type`))
            .headOption

        fulfillmentType match {
            case Some(Fulfillment.Type.Pickup) => Some(OrderChannelType.PhysicalPickup)
            case _ => None
        }
    }

}
