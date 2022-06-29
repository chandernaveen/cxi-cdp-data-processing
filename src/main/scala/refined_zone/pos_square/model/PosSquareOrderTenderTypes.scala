package com.cxi.cdp.data_processing
package refined_zone.pos_square.model

import refined_zone.hub.model.OrderTenderType

object PosSquareOrderTenderTypes {

    final val PosSquareToCxiTenderType: Map[String, OrderTenderType] = Map(
        "CARD" -> OrderTenderType.CreditCard,
        "CASH" -> OrderTenderType.Cash,
        "NO_SALE" -> OrderTenderType.Other,
        "OTHER" -> OrderTenderType.Other,
        "WALLET" -> OrderTenderType.Wallet,
        "SQUARE_GIFT_CARD" -> OrderTenderType.GiftCard
    )

}
