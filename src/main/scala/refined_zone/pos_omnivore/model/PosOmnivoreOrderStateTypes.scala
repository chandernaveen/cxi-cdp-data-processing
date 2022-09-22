package com.cxi.cdp.data_processing
package refined_zone.pos_omnivore.model

import refined_zone.hub.model.OrderStateType

object PosOmnivoreOrderStateTypes {

    final val PosOmnivoreToCxiOrderStateType: Map[String, OrderStateType] = Map(
        "COMPLETED" -> OrderStateType.Completed,
        "OPEN" -> OrderStateType.Open,
        "CANCELLED" -> OrderStateType.Cancelled,
        "UNKNOWN" -> OrderStateType.Unknown
    )

}
