package com.cxi.cdp.data_processing
package refined_zone.pos_toast.model

import refined_zone.hub.model.OrderChannelType

object PosToastOrderChannelTypes {

    final val PosToastToCxiTargetChannelType: Map[String, OrderChannelType] = Map(
        "DINE_IN" -> OrderChannelType.PhysicalLane,
        "TAKE_OUT" -> OrderChannelType.PhysicalPickup,
        "DELIVERY" -> OrderChannelType.PhysicalDelivery
    )

    final val PosToastToCxiOriginateChannelType: Map[String, OrderChannelType] = Map(
        "DINE_IN" -> OrderChannelType.PhysicalLane,
        "TAKE_OUT" -> OrderChannelType.PhysicalPickup
    )

}
