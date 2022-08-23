package com.cxi.cdp.data_processing
package refined_zone.pos_toast.model

import refined_zone.hub.model.OrderTenderType

object PosToastOrderTenderTypes {

    final val PosToastToCxiTenderType: Map[String, OrderTenderType] = Map(
        "CREDIT" -> OrderTenderType.CreditCard,
        "CASH" -> OrderTenderType.Cash,
        "HOUSE_ACCOUNT" -> OrderTenderType.Other,
        "OTHER" -> OrderTenderType.Other,
        "LEVELUP" -> OrderTenderType.Other,
        "UNDETERMINED" -> OrderTenderType.Unknown,
        "REWARDCARD" -> OrderTenderType.GiftCard,
        "GIFTCARD" -> OrderTenderType.GiftCard
    )

}
