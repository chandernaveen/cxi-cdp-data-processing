package com.cxi.cdp.data_processing
package refined_zone.pos_omnivore.normalization

import raw_zone.pos_omnivore.model.OrderType
import refined_zone.hub.model.OrderChannelType
import support.normalization.CxiTaxonomyNormalization

case object OrderChannelTypeNormalization extends CxiTaxonomyNormalization {

    private final val OrigChannelPhysicalLaneOrderTypeName = Seq(
        OrderType.Name.DineIn,
        OrderType.Name.EatIn,
        OrderType.Name.CarryOut
    )

    private final val OrigChannelOtherOrderTypeName = Seq(
        OrderType.Name.Delivery,
        OrderType.Name.OrderAhead
    )

    def normalizeOrderOriginateChannelType(maybeOrderType: Option[OrderType]): OrderChannelType = {
        val maybeOrderTypeName = maybeOrderType.flatMap(orderType => Option(orderType.name))

        maybeOrderTypeName
            .flatMap { orderTypeName =>
                if (OrigChannelPhysicalLaneOrderTypeName.contains(orderTypeName)) {
                    Some(OrderChannelType.PhysicalLane)
                } else if (OrigChannelOtherOrderTypeName.contains(orderTypeName)) {
                    Some(OrderChannelType.Other)
                } else {
                    None
                }
            }
            .getOrElse(OrderChannelType.Unknown) // fallback
    }

    private final val TargetChannelPhysicalLaneOrderTypeName = Seq(
        OrderType.Name.DineIn,
        OrderType.Name.EatIn,
        OrderType.Name.OrderAhead
    )

    private final val TargetChannelPhysicalDeliveryOrderTypeName = Seq(
        OrderType.Name.Delivery
    )

    private final val TargetChannelPhysicalPickupOrderTypeName = Seq(
        OrderType.Name.CarryOut
    )

    def normalizeOrderTargetChannelType(maybeOrderType: Option[OrderType]): OrderChannelType = {
        val maybeOrderTypeName = maybeOrderType.flatMap(orderType => Option(orderType.name))

        maybeOrderTypeName
            .flatMap { orderTypeName =>
                if (TargetChannelPhysicalLaneOrderTypeName.contains(orderTypeName)) {
                    Some(OrderChannelType.PhysicalLane)
                } else if (TargetChannelPhysicalDeliveryOrderTypeName.contains(orderTypeName)) {
                    Some(OrderChannelType.PhysicalDelivery)
                } else if (TargetChannelPhysicalPickupOrderTypeName.contains(orderTypeName)) {
                    Some(OrderChannelType.PhysicalPickup)
                } else {
                    None
                }
            }
            .getOrElse(OrderChannelType.Unknown) // fallback
    }

}
