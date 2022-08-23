package com.cxi.cdp.data_processing
package refined_zone.pos_toast.model

import refined_zone.hub.model.OrderStateType

object PosToastOrderStateTypes {

    final val PosToastToCxiOrderStateType: Map[String, OrderStateType] = Map(
        "NEEDS_APPROVAL" -> OrderStateType.Open,
        "FUTURE" -> OrderStateType.Draft,
        "NOT_APPROVED" -> OrderStateType.Cancelled,
        "APPROVED" -> OrderStateType.Completed
    )

}
