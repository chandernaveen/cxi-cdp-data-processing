package com.cxi.cdp.data_processing
package refined_zone.pos_square.model

import refined_zone.hub.model.OrderStateType

object PosSquareOrderStateTypes {

    final val PosSquareToCxiOrderStateType: Map[String, OrderStateType] = Map(
        "DRAFT" -> OrderStateType.Draft,
        "COMPLETED" -> OrderStateType.Completed,
        "OPEN" -> OrderStateType.Open,
        "CANCELLED" -> OrderStateType.Cancelled
    )

}
